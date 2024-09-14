## Correlación tecnologías 


Este proyecto muestra la correlación entre las diferentes tecnologías que conocen los usuarios de Linkedin con el lenguajes de programación de Scala 

![image](https://github.com/user-attachments/assets/1b6c4397-53dc-4817-ab8d-ae08608cb0f7)


Para la __extracción__ de los datos he usado la api no oficial de Linkedin (en este repositorio tienes más información sobre la extracción de los datos https://github.com/carmenntech/linkedin-search/tree/main ). 

```
pip install linkedin-api
```

En el caso de la __transformación__ de los datos he usado Pyspark y los datos se han insertado en mongodb, las librerías utilizadas son las siguientes:

```
pip install pyspark.sql
pip install mongodb
```

Y por último para el __visionado__ de datos he usado la herramienta de Power bi
