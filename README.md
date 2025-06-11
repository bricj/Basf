## Proceso para disponibilizar la API

1. En la carpeta data debe guardarse el archivo excel con los datos bajos el nombre "data.xlsx"
2. Levantar el contenedor con: docker compose up --build
3. Ejecutar en otra consola:
    docker run --rm -it --net=host -e NGROK_AUTHTOKEN=**token** ngrok/ngrok:latest http 80
4. El forwarding es la URL de la API que se incluye en Copilot Studio

docker run --rm -it --net=host -e NGROK_AUTHTOKEN=2vQ6NfG3h5nwdJ7NDD7ZUfO593a_27xuQKhoh7gnHm2n141GG ngrok/ngrok:latest http 80
https://TU_SUBDOMINIO.ngrok-free.app/data

docker run --rm -it --net=host -e NGROK_AUTHTOKEN=2vQ6NfG3h5nwdJ7NDD7ZUfO593a_27xuQKhoh7gnHm2n141GG ngrok/ngrok:latest http 80 


![Estructura](imgs/estructura.jpg)

![ngrok](imgs/ngrok.jpg)