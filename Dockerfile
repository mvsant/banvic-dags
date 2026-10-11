FROM meltano/meltano:latest-slim

# Define o diretório de trabalho interno
WORKDIR /project

# Copia as definições do projeto para dentro da imagem
COPY meltano.yml .

# Executa a instalação interna dos extratores e loaders durante o build
RUN meltano install

# Comando padrão caso o container seja iniciado solto
CMD ["meltano", "--version"]
