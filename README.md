# CS764 Accelerating Joins with Filters

## Dependencies

Apache arrow is required to run this project. One may download Apache Arrow using

```
brew install apache-arrow
```


## Execution
To run, call

```
>> ./apps/app <SSB query number> <aglorithm> <skew> <SF> 
```

Possible values for <query> are:
  
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

Possible values for <algorithm> are:
  
```
hash
lip
lipk
```

where for `lipk`, `k` is any integer.

Possible values for <skew> are:

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

Possible values for `<SF>` depends on how you generated the SSB dataset. You can technically input any scale factor for which you have the data. However, in this project, we only describe how to generate uniform and skew datasets for `SF = 1`, so please only specify `SF = 1` when running.
