# How to plot or animate zombie curves

First, you need to follow the WiscSee tutorial to get data (valid
ratios). Then, you can follow the steps below to plot or animate
zombie curves. You should be able to easily plot or animate using
other languages/libraries. The code is just an example.

Zombie curves and their animations clearly show a workload's pressure
to an SSD's garbage collector.
Here is an example of the animation, which compares the zombie 
curves of running SQLite-RollBack on ext4 and F2FS. The animation
shows that the curves of both ext4 and F2FS enter a stable state,
where the curve of F2FS indicates much more zombie blocks (i.e.,
blocks with some valid and some invalid data), which give lots 
of pressure to the SSD's garbage collector. The garbage collector
has to frequently move data to free some blocks.

![Zombie Curve Animation](http://pages.cs.wisc.edu/~jhe/zombie-curve-animation-sqlite-rb.gif)
![](http://i.imgur.com/OUkLi.gif)

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

