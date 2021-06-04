### Results updated to the 2021-06-04
## RESULTS STRUCTURE

#### query1_output.csv
Structured as follows:

* Month -> month of the booster vaccination;
* Area -> administration region;
* AVG Vaccinations -> daily average number of administrations per vaccination center in a certain area.

#### query2_output.csv
Structured as follows:

* Date -> first day of the next month;
* Age -> vaccination age group;
* Area -> administration region;
* \#Vaccinations -> number of estimated vaccinations.

#### query3_bisectingkmeans_output.csv & query3_kmeans_output.csv
Structured as follows:

* K -> number of clusters;
* Clusters -> list of areas classified for each cluster. Every list has associated a specific 
  cluster index and, for each area, it's indicated its 
  evaluated percentage of vaccinated population;
* Evaluation -> clustering performance evaluation;
* Processing time -> processing time for the specific algorithm.
