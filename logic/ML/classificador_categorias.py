"""
Sistema de Classificação Automática de Categorias
Fase 1: TF-IDF + Similaridade de Coseno
Preparado para evolução para Random Forest
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
import re
from collections import defaultdict
import pickle
import os
from datetime import datetime

class ClassificadorCategorias:
    """
    Classificador evolutivo de categorias para transações financeiras.
    
    Fase 1: TF-IDF + Similaridade
    Fase 2: Random Forest + NLP (futuro)
    """
    
    def __init__(self, versao="tfidf"):
        self.versao = versao
        self.modelo_treinado = False
        self.historico_treinamento = []
        self.feedback_usuario = []
        self.estatisticas = {}
        
        # Configurações TF-IDF
        self.vocabulario = set()
        self.idf_scores = {}
        self.categoria_vectores = {}
        
        # Palavras-chave por categoria (regras básicas)
        self.palavras_chave = {
            "2.1.2 - PGTO COMBUSTÍVEL/REVENDA": [
                "posto", "shell", "br", "ipiranga", "combustivel", "gasolina", "diesel"
            ],
            "2.1.1 - PGTO SUPERMERCADO": [
                "supermercado", "giassi", "angeloni", "walmart", "mercado", "super"
            ],
            "1.9 - TED/DOC/PIX": [
                "pix", "ted", "doc", "transferencia", "recebido", "enviado"
            ],
            "2.1.4 - PGTO FARMÁCIA": [
                "farmacia", "drogaria", "nissei", "catarinense", "remedio"
            ],
            "2.1.7 - FORNECEDOR LOJA/REVENDA": [
                "fornecedor", "atacado", "distribuidora", "comercial"
            ]
        }
    
    def preprocessar_texto(self, texto: str) -> str:
        """Limpa e prepara texto para análise"""
        if not texto or pd.isna(texto):
            return ""
        
        # Converter para minúsculas
        texto = str(texto).lower()
        
        # Remover caracteres especiais, manter apenas letras e espaços
        texto = re.sub(r'[^a-záàâãéèêíìîóòôõúùûç\s]', ' ', texto)
        
        # Remover espaços extras
        texto = re.sub(r'\s+', ' ', texto).strip()
        
        return texto
    
    def extrair_features_simples(self, transacao: Dict) -> Dict:
        """Extrai features básicas da transação (Fase 1)"""
        descricao = self.preprocessar_texto(transacao.get('descricao', ''))
        valor = float(transacao.get('valor_absoluto', 0))
        
        return {
            'descricao_limpa': descricao,
            'palavras': descricao.split(),
            'valor': valor,
            'faixa_valor': self._classificar_faixa_valor(valor),
        }
    
    def _classificar_faixa_valor(self, valor: float) -> str:
        """Classifica valor em faixas"""
        if valor < 50:
            return "baixo"
        elif valor < 200:
            return "medio"
        elif valor < 1000:
            return "alto"
        else:
            return "muito_alto"
    
    def treinar_modelo_tfidf(self, dados_historico: pd.DataFrame):
        """Treina modelo TF-IDF com dados históricos"""
        print("🤖 Treinando classificador TF-IDF...")
        
        # Preparar dados
        dados_limpos = []
        categorias = []
        
        for _, row in dados_historico.iterrows():
            if pd.notna(row.get('categoria')) and pd.notna(row.get('descricao')):
                descricao_limpa = self.preprocessar_texto(row['descricao'])
                if descricao_limpa:  # Só adicionar se tem conteúdo
                    dados_limpos.append(descricao_limpa)
                    categorias.append(row['categoria'])
        
        print(f"📊 {len(dados_limpos)} registros válidos para treinamento")
        
        if len(dados_limpos) < 10:
            print("⚠️ Poucos dados para treinamento - usando regras básicas")
            self.modelo_treinado = True
            return
        
        # Construir vocabulário
        self.vocabulario = set()
        documentos_por_categoria = defaultdict(list)
        
        for descricao, categoria in zip(dados_limpos, categorias):
            palavras = descricao.split()
            self.vocabulario.update(palavras)
            documentos_por_categoria[categoria].append(descricao)
        
        # Calcular TF-IDF simplificado para cada categoria
        self.categoria_vectores = {}
        total_docs = len(dados_limpos)
        
        for categoria, docs in documentos_por_categoria.items():
            # Term Frequency para esta categoria
            palavra_freq = defaultdict(int)
            total_palavras = 0
            
            for doc in docs:
                for palavra in doc.split():
                    palavra_freq[palavra] += 1
                    total_palavras += 1
            
            # Normalizar frequências
            categoria_vector = {}
            for palavra in self.vocabulario:
                tf = palavra_freq[palavra] / max(total_palavras, 1)
                
                # IDF simplificado: log(total_docs / docs_com_palavra)
                docs_com_palavra = sum(1 for doc in dados_limpos if palavra in doc)
                idf = np.log(total_docs / max(docs_com_palavra, 1))
                
                categoria_vector[palavra] = tf * idf
            
            self.categoria_vectores[categoria] = categoria_vector
        
        self.modelo_treinado = True
        self.historico_treinamento = dados_limpos
        
        print(f"✅ Modelo treinado com {len(self.categoria_vectores)} categorias")
        print(f"📚 Vocabulário: {len(self.vocabulario)} palavras")
    
    def calcular_similaridade(self, texto: str, categoria_vector: Dict) -> float:
        """Calcula similaridade entre texto e vetor de categoria"""
        palavras = self.preprocessar_texto(texto).split()
        if not palavras:
            return 0.0
        
        # Vetor do texto
        texto_vector = defaultdict(float)
        for palavra in palavras:
            texto_vector[palavra] += 1.0 / len(palavras)
        
        # Similaridade de coseno simplificada
        produto_escalar = 0.0
        norma_texto = 0.0
        norma_categoria = 0.0
        
        todas_palavras = set(texto_vector.keys()) | set(categoria_vector.keys())
        
        for palavra in todas_palavras:
            v_texto = texto_vector.get(palavra, 0.0)
            v_categoria = categoria_vector.get(palavra, 0.0)
            
            produto_escalar += v_texto * v_categoria
            norma_texto += v_texto ** 2
            norma_categoria += v_categoria ** 2
        
        if norma_texto == 0 or norma_categoria == 0:
            return 0.0
        
        return produto_escalar / (np.sqrt(norma_texto) * np.sqrt(norma_categoria))
    
    def sugerir_categoria(self, transacao: Dict) -> Dict:
        """Sugere categoria para uma transação"""
        if not self.modelo_treinado:
            return {
                'categoria_sugerida': None,
                'confianca': 0.0,
                'alternativas': [],
                'motivo': 'Modelo não treinado'
            }
        
        descricao = transacao.get('descricao', '')
        
        # Calcular similaridades
        scores = {}
        for categoria, vector in self.categoria_vectores.items():
            similaridade = self.calcular_similaridade(descricao, vector)
            
            # Boost para palavras-chave específicas
            boost = self._calcular_boost_palavras_chave(descricao, categoria)
            scores[categoria] = min(similaridade + boost, 1.0)
        
        if not scores:
            return {
                'categoria_sugerida': None,
                'confianca': 0.0,
                'alternativas': [],
                'motivo': 'Nenhuma categoria encontrada'
            }
        
        # Ordenar por score
        categorias_ordenadas = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        melhor_categoria, melhor_score = categorias_ordenadas[0]
        
        # Alternativas (top 3)
        alternativas = [(cat, score) for cat, score in categorias_ordenadas[1:4]]
        
        return {
            'categoria_sugerida': melhor_categoria,
            'confianca': melhor_score,
            'alternativas': alternativas,
            'motivo': f'Similaridade: {melhor_score:.1%}'
        }
    
    def _calcular_boost_palavras_chave(self, descricao: str, categoria: str) -> float:
        """Adiciona boost para palavras-chave específicas"""
        descricao_limpa = self.preprocessar_texto(descricao)
        palavras_desc = set(descricao_limpa.split())
        
        palavras_categoria = set(self.palavras_chave.get(categoria, []))
        intersecao = palavras_desc & palavras_categoria
        
        if intersecao:
            return 0.2 * len(intersecao) / len(palavras_categoria)
        
        return 0.0
    
    def registrar_feedback(self, transacao: Dict, categoria_sugerida: str, 
                          categoria_correta: str, aceito: bool):
        """Registra feedback do usuário para melhorar modelo"""
        feedback = {
            'timestamp': datetime.now(),
            'transacao': transacao,
            'categoria_sugerida': categoria_sugerida,
            'categoria_correta': categoria_correta,
            'aceito': aceito
        }
        self.feedback_usuario.append(feedback)
        
        # Atualizar estatísticas
        self._atualizar_estatisticas()
    
    def _atualizar_estatisticas(self):
        """Atualiza estatísticas de desempenho"""
        if not self.feedback_usuario:
            return
        
        total = len(self.feedback_usuario)
        aceitos = sum(1 for f in self.feedback_usuario if f['aceito'])
        
        self.estatisticas = {
            'total_sugestoes': total,
            'aceitas': aceitos,
            'precisao': aceitos / total if total > 0 else 0,
            'data_ultima_atualizacao': datetime.now()
        }
    
    def salvar_modelo(self, caminho: str):
        """Salva modelo treinado"""
        dados = {
            'versao': self.versao,
            'modelo_treinado': self.modelo_treinado,
            'vocabulario': self.vocabulario,
            'categoria_vectores': self.categoria_vectores,
            'palavras_chave': self.palavras_chave,
            'feedback_usuario': self.feedback_usuario,
            'estatisticas': self.estatisticas
        }
        
        with open(caminho, 'wb') as f:
            pickle.dump(dados, f)
    
    def carregar_modelo(self, caminho: str):
        """Carrega modelo salvo"""
        if not os.path.exists(caminho):
            return False
        
        try:
            with open(caminho, 'rb') as f:
                dados = pickle.load(f)
            
            self.versao = dados.get('versao', 'tfidf')
            self.modelo_treinado = dados.get('modelo_treinado', False)
            self.vocabulario = dados.get('vocabulario', set())
            self.categoria_vectores = dados.get('categoria_vectores', {})
            self.palavras_chave = dados.get('palavras_chave', {})
            self.feedback_usuario = dados.get('feedback_usuario', [])
            self.estatisticas = dados.get('estatisticas', {})
            
            return True
        except Exception as e:
            print(f"❌ Erro ao carregar modelo: {e}")
            return False


# Funções auxiliares para integração com Streamlit
def treinar_classificador_com_dados_mr(dados_mr: pd.DataFrame) -> ClassificadorCategorias:
    """Treina classificador com dados do sistema MR"""
    classificador = ClassificadorCategorias(versao="tfidf")
    classificador.treinar_modelo_tfidf(dados_mr)
    return classificador

def sugerir_categorias_para_transacoes(transacoes_novas: pd.DataFrame, 
                                     classificador: ClassificadorCategorias) -> pd.DataFrame:
    """Adiciona sugestões de categoria para transações novas"""
    df_com_sugestoes = transacoes_novas.copy()
    
    sugestoes = []
    confiancas = []
    alternativas = []
    
    for _, transacao in transacoes_novas.iterrows():
        resultado = classificador.sugerir_categoria(transacao.to_dict())
        
        sugestoes.append(resultado['categoria_sugerida'])
        confiancas.append(resultado['confianca'])
        alternativas.append(resultado['alternativas'])
    
    df_com_sugestoes['categoria_sugerida'] = sugestoes
    df_com_sugestoes['confianca_sugestao'] = confiancas
    df_com_sugestoes['alternativas'] = alternativas
    
    return df_com_sugestoes