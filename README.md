# Arquitetura híbrida de compressão de dados sem perdas em gateway inteligente

Repositório de códigos, dados e experimentos do trabalho que investiga uma arquitetura de compressão em múltiplos estágios para redes IoT baseadas em LoRa, usando um **gateway inteligente** (Raspberry Pi 5) para re-compressão avançada de pacotes já comprimidos nos nós.

---

## Visão geral da arquitetura

<!-- Atualize o caminho abaixo conforme a organização do repositório -->
![Arquitetura híbrida de compressão em gateway inteligente](docs/img/arquitetura_gateway.png)

Na arquitetura proposta:

- **Nós IoT** executam compressão leve (LZW/Huffman) e enviam pacotes já comprimidos.
- O **gateway (Raspberry Pi 5)** atua como camada de processamento avançada:
  1. Agrupa centenas de pacotes comprimidos em um **contêiner binário com metadados**;
  2. Aplica algoritmos de **re-compressão de alto desempenho** sobre o contêiner;
  3. Transmite o arquivo resultante pelo backhaul (Ethernet/satélite/celular) até a nuvem.
- Na nuvem, os dados podem ser descomprimidos e desempacotados preservando a integridade de cada mensagem original.

Essa organização busca **reduzir tráfego, uso de banda e memória**, mantendo a viabilidade energética dos nós IoT.

---

## Motivação

A Internet das Coisas gera grandes volumes de dados em dispositivos com:

- bateria limitada,  
- pouco processamento,  
- pouca memória.

Em redes LoRa, cada pacote tem carga útil reduzida (por exemplo, **222 bytes em SF7**), o que exige muitos quadros para transmitir séries temporais longas. A compressão sem perdas ajuda, mas:

- Algoritmos **clássicos** (LZW, Huffman) são energeticamente eficientes, porém têm compressão moderada.
- Algoritmos **modernos** (CMIX, PAQ8PX, GMIX, etc.) comprimem muito mais, mas exigem **CPU e RAM** que os nós não possuem.

A solução proposta é deslocar os compressores pesados para o **gateway**, mantendo compressão leve nos nós.

---

## Objetivo do projeto

Implementar e avaliar uma **arquitetura híbrida de compressão em múltiplos estágios** em que:

1. **Nós IoT** comprimem localmente os dados com **LZW** ou **Huffman**.
2. O **gateway Raspberry Pi 5**:
   - empacota centenas de arquivos já comprimidos em um **contêiner único** com metadados padronizados;
   - re-comprime esse contêiner com algoritmos avançados (GMIX, CMIX, PAQ8PX, LSTM-Compress, BSC-m03 ou LZW);
3. Avalia-se:
   - **taxa de compressão**,  
   - **tempo de execução**,  
   - **uso de CPU**,  
   - **consumo de memória**.

---

## Dados utilizados

Três tipos de dados IoT, representando aplicações reais:

- **GPS** – coordenadas numéricas (latitude/longitude);
- **IoT diversificada** – leituras de sensores (temperatura, umidade, luminosidade, etc.);
- **Logística** – identificadores alfanuméricos para rastreamento de produtos.

Cada tipo de dado é usado em **dois arquivos de entrada**:

- comprimido por **Huffman** (mensagens agrupadas até 222 bytes);
- comprimido por **LZW** (mensagens agrupadas até 222 bytes).

Exemplo (resumo da Tabela do artigo):

| Algoritmo | Tipo de dado       | Mensagens agrupadas | Tamanho (bytes) |
|-----------|--------------------|---------------------|-----------------|
| Huffman   | GPS                | 18                  | 200             |
| Huffman   | IoT diversificada  | 9                   | 116             |
| Huffman   | Logística          | 22                  | 220             |
| LZW       | GPS                | 25                  | 186             |
| LZW       | IoT diversificada  | 35                  | 222             |
| LZW       | Logística          | 26                  | 216             |

Esses arquivos já respeitam o limite de carga útil do LoRa em SF7.

---

## Algoritmos de compressão

### Nos nós IoT (compressão inicial)

- **Huffman**  
- **Lempel–Ziv–Welch (LZW)**  

São algoritmos clássicos, de **baixo custo computacional**, adequados para dispositivos com poucos recursos.

### No gateway (re-compressão)

Algoritmos modernos e mais pesados, aplicados sobre o contêiner:

- **BSC / BSC-m03** – compressão por ordenação de blocos (BWT + CSE); rápido e com boa taxa de compressão.  
- **LZW** – reutilizado como re-compressor de complexidade moderada.  
- **LSTM-Compress** – compressão baseada em rede LSTM acoplada à codificação aritmética.  
- **GMIX** – evolução modular do CMIX, com mistura de modelos probabilísticos.  
- **PAQ8PX** – combinação de centenas de modelos especializados + rede neural para predição bit a bit.  
- **CMIX** – compressor de altíssimo desempenho (pré-processamento complexo + milhares de modelos), analisado separadamente pelo **custo extremo de CPU e RAM**.

---

## Metodologia experimental

Todos os testes usam uma **Raspberry Pi 5** (Cortex-A76 quad-core, 8 GB de RAM, Raspberry Pi OS 64-bit).

### Formato de contêiner

Os arquivos já comprimidos são empacotados em um contêiner binário com metadados:

```text
[u32 N]                                    # número de arquivos
N * [
  u16 len_nome  | nome
  u64 len_dados | dados
]
