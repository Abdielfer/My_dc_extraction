# Tests de l'extraction wcs sur le service d'élévation    

La méthode mesure le temps de réponse du processus complet d'extraction en fonction du nombre de pixels.   
Le test commence avec un pixel size de 1000 m et test la même zone en réduisant le pixel size de 25 m à chaque itération jusqu'à ce que l'extraction échoue.    
L'erreur arrive toujours proche d'un pixel size de 125m pour environ 95 millions de pixels.   
L'erreur arrive à cause du time out de 120 secondes qu'on a mis.   
Le test est fait sur la fonction : **wcs_coverage_extract**     

Voici différentes itérations du même test: 


![test 1](/extract/monitoring/wcs_extract/t1.png)
![test 2](/extract/monitoring/wcs_extract/t2.png)
![test 3](/extract/monitoring/wcs_extract/t3.png)
![test 4](/extract/monitoring/wcs_extract/t4.png)
![test 5](/extract/monitoring/wcs_extract/t5.png)



 * le t5 a été fait avec des incréments de 10 m.  
