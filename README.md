# CS764 Accelerating Joins with Filters

This document presents the installation guide of our project and its usage. By far we have only tested our project on macOS.

## Overview

* Our final report can be found at `docs/rep.pdf`.
* All code can is in `src` (library code), `apps` (executable code), and `util` (helper code) directories.
* `benchmarks/benchmark-1` contains SSB data generated from [here](https://github.com/UWQuickstep/SQL-benchmark-data-generator/tree/master/ssbgen) with SF = 1.
* `benchmarks/benchmark-skew` contains all skew LINEORDER tables discussed in the report as well as several more.
* `scripts` contains bash/python scripts used to collect, analyze, and plot results.
* 

## Installation

Apache arrow is required to run this project. One may download Apache Arrow using

```
brew install apache-arrow
```

Next clone our project and build our project. 
Note that this repository contains all of our experimental datasets (several GB),
so this will take a minute or so to clone.

```
git clone https://github.com/NicholasCorrado/LIP.git
cd LIP
cmake .
make
```


## Execution
To run, call

```
./apps/main <SSB query number> <aglorithm> <skew> <SF> 
```

Possible values for query> are:
  
```
1.1
1.2
1.3
2.1
2.2
2.3
3.1
3.2
3.3
3.4
4.1
4.2
4.3
```

Possible values for `<algorithm>` are:
  
```
hash
lip
lipk
```

where k can be substituted by any positive integer.

Possible values for `<skew>` are:

```
uniform
skew-date-5-5
skew-date-10-10
skew-date-20-20
skew-date-50-50
skew-date-50-10
skew-date-10-50
skew-date-first-half
skew-date-linear
skew-date-linear-part-20-20
skew-date-linear-part-20-20-supp-10-20
```

Possible values for `<SF>` depends on how you generated the SSB dataset. You can technically input any scale factor for which you have the data. However, in this project, we only describe how to generate uniform and skew datasets for SF = 1, so please only specify SF = 1 when running.

Here is how you would run query 4.2 using LIP-42 on dataset skew-date-50-50 with SF = 1:

```
>> ./apps/main 4.2 lip42 skew-date-50-50 1
```

The output will look something like 

```
Running query 4.2 ...
CR 1.22494
Rows 48141
RunningTime 1374760
```

Where CR is the competitive ratio (defined in the report), Rows is the number of rows in the LINEORDER table that would be joined, and RunningTime is the running time in milliseconds.
