# How to plot or animate zombie curves

First, you need to follow the WiscSee tutorial to get data (valid
ratios). Then, you can follow the steps below to plot or animate
zombie curves. You should be able to easily plot or animate using
other languages/libraries. The code is just an example.

## Example: Plot Zombie Curve

1. Install R

2. Install the following packages in R

```
ggplot2
jsonlite
reshape2
plyr
```

3.  Do the following in the current directory (in shell). `Rscript` is 
a command to run R scripts.

```
$ Rscript ./plot-zombie.r
```

The step above will generate a file "plot.pdf", which is the zombie curve.

4. You should be able to easily modify the code in `plot-zombie.r` to plot your data, 
even if you do not know R.


## Example: Animate Zombie Curves

1. Install R

2. Install the following packages in R

```
library(ggplot2)
library(dplyr)
library(plyr)
library(reshape2)
library(jsonlite)
library(gganimate)
```

3.  Do the following in the current directory (in shell). `Rscript` is 
a command to run R scripts. It will take a few minutes because R will
have to generate hundreds of GIFs and assemble them to one file.

```
$ Rscript ./plot-zombie-animation.r
```

The step above will generate a file "zombie-curve-animation.gif", which 
is the zombie curve animation. You should be able to view it by opening it
with a broswer.

4. You should be able to easily modify the code in `plot-zombie-animation.r` to 
animate your data, even if you do not know R.

