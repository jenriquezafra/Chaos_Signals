# usamos una imagen oficial de python 3.12 como base
FROM python:3.12-slim

# establecemos el directorio de trabajo
WORKDIR /app

# copiamos los archivos necesarios al contenedor
COPY requirements.txt .

# instalamos las dependencias
RUN pip install --upgrade pip && pip install -r requirements.txt

# copiamos el resto de los archivos al contenedor
COPY src/ src/
COPY notebooks/ notebooks/

# exponemos el puerto 8888 para Jupyter Notebook
EXPOSE 8888 

# comando para iniciar Jupyter Notebook
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
