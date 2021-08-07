## Laying the foundation stone for an Open Data catalog


Let’s assume that we’re going to create an Open Data catalog from scratch. We’re ambitious, and we’re thinking about collecting data (including location information) from many sources in order to integrate it into CARTO and make it publically available. In this exercise you’ll lay the foundation stone for that. Even though you’ll only prepare one dataset, think about it as the first step of a bigger project.

This test should take you around 4-5 hours. Don’t worry if you finish faster, or if it takes you a little longer, as being comfortable with the tooling matters. As you’ve been told, we’d like you to send your work to us in the following 7 days.

Keep in mind the following:
- The more you explain the decisions that you made, the better. We want to know the way you think. This is especially important on tradeoffs.
- Please, make it easy to run and test.
- Although files are not big, come up with an approach that can be valid, both in time and resources, for bigger datasets.
- We encourage you to contact us. We know that you will have doubts and comments and knowing more about the way you think to solve complex problems and communicating them is also part of the test. We’ll do our best to answer as quickly as possible. You can reach us at juanignaciosl@carto.com and acarlon@carto.com (CC both of us and also jmartincorral@carto.com)

Your first goal is to **write a Python 3 program to import the spanish census data into a PostgreSQL + PostGIS database**. Feel free to use any libraries you deem appropriate for the task, as long as they are available under an open source license.

Your program should:
- Download the spanish census data from 2011, which is available at http://www.ine.es/censos2011_datos/cen11_datos_resultados_seccen.htm. There are three relevant files:
  - Description of the data ([Relación de indicadores disponibles](http://www.ine.es/censos2011_datos/indicadores_seccen_rejilla.xls)). It contains a mapping from field codes to human-readable descriptions.
  - The data itself, available as a zip files of CSV and XLS formatted files. ([Ficheros por ccaa en formato CSV/XLS](http://www.ine.es/censos2011_datos/indicadores_seccion_censal_csv.zip)). 
  - The geometry of the Municipalities (muni) available as SHP files. ([Datos del Centro Nacional de Información Geográfica](http://centrodedescargas.cnig.es/CentroDescargas/descargaDir?secDescDirLA=114023&pagActual=1&numTotReg=5&codSerieSel=CAANE)).
- Import the data into a PostgreSQL/PostGIS instance. The result should be two tables, one for the geometry and a second one for the statistical data.
- For importing the geometry data, you can use your preferred tool. We suggest using the [ogr2ogr](http://www.gdal.org/ogr2ogr.html) command, which comes with GDAL and supports importing shapefiles into PostGIS. There are also GDAL bindings for python which allows for more flexibility, at the expense of ease of use. (Hint: If you get import errors, you may need to pass the following options to ogr2ogr: `-nlt MultiPolygon -lco PRECISION=no`)
- You should combine the data and the description files from the previous step in order to create a table with the statistical indicators.

Additionally, we want to **write a couple of SQL queries to access that data**. We want to be able to answer the following questions:
- Get the population density of each of the municipalities of Madrid
- Get the names of the 10 provinces with the highest percentage of people with university degrees (third-level studies)


