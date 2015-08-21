GIT Branches
-------------------------

- master
- dev
- exp.snapshot

exp.snapshot should be the most active branch, where you write code, commit
before running an experiment. 

Th useful code changes in exp.snapshot will go to dev branch. Here you may
need to merge commits and split out the useful changes you've made. You don't
want those trivial experiment-unique settings go to dev (and eventually
master branch).

You may have different dev branches (dev.try.A, dev.try.B, ...). If you feel 
good about your dev branch, you can merge it to master. If you have different
dev branches you want to merge, you may need to resolve conflicts. 

Remeber, exp.snapshot can be messy. dev and master should be clean.

Try to use only one dev and exp.snapshot for this project. It will be easier.

**Make it beautiful to make it useful!**
