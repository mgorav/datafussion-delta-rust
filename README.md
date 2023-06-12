## Data Fusion Query Engine (NO SPARK) For DeltaLake

I this blog, I will talk about query delta files without using SPARK. Yes, you heard it right. This potentially 
open us opportunities to build data centric web analytics/machine learning application as well as move "CUBES" to 
technologies which can scale in memory. 

`NOTE: The query mechanism can be extended to other format like paraquet, csv, json etc`

Few years back, if I had said: _"I want to create by database for cubes just like the way FANG companies does it"_. It 
have been deemed an over engineering project. We can do it now easily fitting, need company. No point buying a product 
and spend n number of man hours to deploy, manage and monitoring, still fighting to move old CUBES to new technologies 
(for scale).

I this blog I will utilize system programming language RUST and columnar storage (apt for analytics).

