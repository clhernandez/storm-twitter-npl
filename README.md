Storm Twitter Sentiment Analysis
========================

Links
* Storm author [Nathan Marz's github repo](https://github.com/nathanmarz/storm)
* Storm tutorial [mbonacci](https://github.com/mbonaci/mbo-storm)

Funcionalidad:
Realiza un analisis del sentimiento de cada tweet recibido mediante la libreria Stanford Core-NPL. Solo funciona con tweets en ingles.

Para instalar:
* Clonar este repo
* Ejecutar: mvn clean install
* Importar en eclipse
* Es necesario configurar los tokens OAuth https://dev.twitter.com
* Agregar los tokens de configuracion en el archivo config/twitter4j.properties
* Ejecutar Topology.java

Profit.

FIX DATE FROM MONGO
var cursor = db.tweets.find()
while(cursor.hasNext()){ 
	var doc = cursor.next(); 
	db.tweets.update({_id : doc._id}, {$set: {created_at: new Date(doc.created_at)}}) 
}

mongoimport --db storm-world-cup --collection tweets --file tweet-part3.json
