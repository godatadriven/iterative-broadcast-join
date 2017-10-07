# The iterative broadcast join
The iterative broadcast join example code.

## How to run the code

First generate a dataset:
```
sbt "run generate"
```
By generating the data first in a separate job, we ensure that we use the same data and the benchmark isn't affected by the data generation process. Then you can run the benchmark:
```
sbt "run benchmark"
```
