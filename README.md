## Atividade

Adicione uma novo ator (microsserviço) no projeto que será responsável por notificar através do telegram ou e-mail que a operação do 'rotate' ou 'grayscale' foi finalizada. Para isso será necessário alterar o projeto adicionando uma nova etapa de pubicação num novo tópico (por exemplo **/notificacao**) por parte do microsserviço 'rotate' e 'grayscale'. O novo microsserviço '**notifacador**' será responsável por checar (pooling) o tópico e fazer o envio de mensagem no telegram ou e-mail para um contato defindo (pode ser fixo ou variável**) quando a operação estiver finalizada. 
** Se fizer variável, coloque um input de e-mail/telegram_id no HTML do microsserviço 'upload'. 

As mensagens enviadas devem conter:
  1. o nome do arquivo original
  2. a indicação da operação realizada

Por exemplo: 
```
O arquivo perfil.jpg foi rotacionado.
```
```
O arquivo perfil.jpg foi transformado em preto e branco.
```

Sugestões de como usar Telegram/Email: 

* Telegram: 
   * https://usp-python.github.io/05-bot/
   * https://stackoverflow.com/questions/43291868/where-to-find-the-telegram-api-key
  
* E-mail:
   * https://realpython.com/python-send-email/

Ao terminar os experimentos, lembre-se de executar ```docker-compose down```


## Artigos que foram base para o projeto

- Exemplo de código com Kafka < https://betterprogramming.pub/a-simple-apache-kafka-cluster-with-docker-kafdrop-and-python-cf45ab99e2b9 >

- Exemplo de programa em Flask com upload de imagem < https://github.com/roytuts/flask/tree/master/python-flask-upload-display-image >

## Material Complementar

[Arquitetura Publish/Subscribe](https://engsoftmoderna.info/cap7.html#arquiteturas-publishsubscribe)

[Entendo o Kafka](https://vepo.medium.com/entendendo-o-kafka-bf64169e421f)

[Apache Kafka](https://medium.com/trainingcenter/apache-kafka-838882261e83)

[Apache Kafka: Aprendendo na prática](https://medium.com/trainingcenter/apache-kafka-codifica%C3%A7%C3%A3o-na-pratica-9c6a4142a08f)


# Atenção!
É necessário criar o arquivo *config.py* na pasta "notificator-app" com as seguinte variáveis:
api_key: Com o valor da API Bot do telegram.
email_sender: O email responsável para enviar as notificações.
password: senha do email.