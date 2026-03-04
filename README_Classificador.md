# 🤖 Sistema de Classificação Automática de Categorias

## 📋 Visão Geral

Sistema evolutivo de inteligência artificial para sugestão automática de categorias em transações financeiras OFX, baseado no histórico do sistema MR.

## 🚀 Funcionalidades

### ✅ **FASE 1: TF-IDF + Similaridade (ATUAL)**
- **Algoritmo**: TF-IDF com Similaridade de Coseno
- **Precisão**: ~70-85% (dependendo dos dados)
- **Velocidade**: ⚡⚡⚡ Muito rápida
- **Base**: Análise de texto das descrições + regras de palavras-chave

### 🔮 **FASE 2: Random Forest + NLP (FUTURO)**
- **Algoritmo**: Random Forest com múltiplas features
- **Precisão**: ~90%+ esperada
- **Features**: Texto + Valor + Padrões temporais + Banco + Histórico
- **Aprendizado**: Contínuo com feedback do usuário

## 🎯 Como Funciona

### 1. **Treinamento Automático**
```python
# Sistema busca dados históricos do MR
dados_mr = buscar_lancamentos_e_transferencias_api(empresas)

# Treina classificador com descrições + categorias
classificador = treinar_classificador_com_dados_mr(dados_mr)

# Modelo pronto para uso
```

### 2. **Sugestão Automática**
```python
# Para cada transação nova OFX
transacao = "POSTO SHELL ARARANGUÁ"

# Sistema sugere categoria
resultado = {
    'categoria_sugerida': '2.1.2 - PGTO COMBUSTÍVEL/REVENDA',
    'confianca': 0.89,  # 89%
    'alternativas': [
        ('2.1.7 - FORNECEDOR LOJA/REVENDA', 0.12),
        ('1.9 - TED/DOC/PIX', 0.05)
    ]
}
```

### 3. **Interface com Revisão**
- 🟢 **Alta confiança (>80%)**: Aplicação automática sugerida
- 🟡 **Média confiança (60-80%)**: Revisão recomendada  
- 🔴 **Baixa confiança (<60%)**: Categorização manual
- ❓ **Sem sugestão**: Casos não identificados

## 📊 Níveis de Confiança

| Cor | Faixa | Significado | Ação Recomendada |
|-----|-------|-------------|-------------------|
| 🟢 | >80% | Alta confiança | ✅ Aplicar automaticamente |
| 🟡 | 60-80% | Média confiança | 👀 Revisar sugestão |
| 🔴 | <60% | Baixa confiança | ✏️ Categorizar manualmente |
| ❓ | — | Sem sugestão | 🔍 Análise manual necessária |

## 🎮 Como Usar

### 1. **Upload do OFX**
- Faça upload normal do arquivo OFX
- Sistema detecta duplicatas primeiro

### 2. **Ativação da IA**
- Na aba "✨ Novas Transações"
- Marque: ☑️ "Ativar sugestões automáticas de categoria"
- Sistema treina automaticamente

### 3. **Revisão das Sugestões**
- Visualize sugestões na tabela
- Aceite sugestões de alta confiança
- Revise sugestões de média confiança
- Categorize manualmente as sem sugestão

### 4. **Download Inteligente**
- 📥 **Download Completo**: Todas as transações
- 🟢 **Download Alta Confiança**: Apenas sugestões confiáveis

## 🔧 Configurações Avançadas

### **Re-treinar Modelo**
- Botão: 🔄 "Treinar Modelo"
- Usa dados mais recentes do MR
- Melhora precisão com novos padrões

### **Feedback Contínuo**
- Correções manuais alimentam o sistema
- Modelo aprende com suas escolhas
- Próximas importações ficam mais precisas

## 📈 Evolução Planejada

### **Fase Atual → Fase Futura**

| Aspecto | TF-IDF (Atual) | Random Forest (Futuro) |
|---------|----------------|------------------------|
| **Algoritmo** | Similaridade de texto | Múltiplas árvores de decisão |
| **Features** | Apenas descrição | Texto + Valor + Tempo + Banco |
| **Precisão** | 70-85% | 90%+ esperada |
| **Explicabilidade** | ⭐⭐⭐ | ⭐⭐ |
| **Dados necessários** | 📊 | 📊📊📊 |

### **Benefícios da Evolução**
- ✅ **Zero retrabalho**: Mesma interface
- ✅ **Dados aproveitados**: Todo feedback coletado é usado
- ✅ **Melhoria contínua**: Sistema fica mais inteligente
- ✅ **Múltiplos fatores**: Não apenas texto, mas contexto completo

## 🎯 Resultados Esperados

### **Economia de Tempo**
- ⏱️ **Antes**: 100% categorização manual
- ⏱️ **Depois**: ~80% automático, 20% revisão

### **Consistência**
- 📐 **Padronização**: Categorias consistentes com histórico
- 🎯 **Precisão**: Redução de erros humanos
- 📊 **Métricas**: Acompanhamento de performance

### **Aprendizado**
- 🧠 **Inteligente**: Aprende com cada correção
- 🔄 **Evolutivo**: Melhora continuamente
- 🎪 **Adaptativo**: Se ajusta aos padrões da empresa

## 💡 Dicas de Uso

1. **Primeira Vez**: Deixe o sistema treinar com dados recentes
2. **Alta Confiança**: Confie nas sugestões 🟢, elas são bem precisas
3. **Correções**: Sempre corrija sugestões erradas - isso ensina o sistema
4. **Re-treinamento**: Faça periodicamente para incorporar novos padrões
5. **Feedback**: Quanto mais usar, melhor fica!

## 🏗️ Arquitetura Técnica

```
📁 logic/ML/
└── classificador_categorias.py    # Módulo principal do classificador

🔧 Componentes:
├── ClassificadorCategorias        # Classe principal
├── treinar_classificador_com_dados_mr()  # Função de treinamento
└── sugerir_categorias_para_transacoes()  # Função de predição

🧠 Algoritmos:
├── TF-IDF Vectorization          # Conversão texto → números
├── Cosine Similarity             # Cálculo de similaridade
├── Keyword Boosting              # Boost para palavras-chave
└── Confidence Scoring            # Cálculo de confiança
```

---

**🚀 Sistema pronto para evolução: De TF-IDF simples para Random Forest inteligente!**