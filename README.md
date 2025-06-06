## Proceso para disponibilizar la API

1. En la carpeta data debe guardarse el archivo excel con los datos bajos el nombre "data.xlsx"
2. Levantar el contenedor con: docker compose up --build
3. Ejecutar en otra consola:
    docker run --rm -it --net=host -e NGROK_AUTHTOKEN=**token** ngrok/ngrok:latest http 80
4. El forwarding es la URL de la API que se incluye en Copilot Studio