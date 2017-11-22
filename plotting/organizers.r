# This class finds all the 'filename' in the experiment directory
# and creates an analyzer object on each of them. Of course, we then
# run the analysis.
# For example:
# diranalyzer = DirAnalyzer$new(expname='randseq', filename="timeline.txt")
# diranalyzer$analyze_each(TimelineRC)
DirAnalyzer <- setRefClass("DirAnalyzer",
  fields = list(expname="character", filename="character"),
  methods = list(
    analyze_each = function(analysis_class)
    {
        exp_path = get_exp_path(expname)
        files = get_file_paths_recursively(exp_path, filename)

        for (filepath in files) {
            analyze_obj = analysis_class$new(filepath=filepath)
            analyze_obj$main()
        }
    })
)

# Iterate all subexperiement in an experiment,
# and allow an analyzer to analyze the files in the subexp dir
# possibly using multiple files in the subexp dir
#
# Example
# subexpiter = SubExpIter(exp_rel_path='parallelgc002')
# subexpiter$iter_each_subexp(SubExpTimelineAnalyzer)
SubExpIter <- setRefClass("SubExpIter",
    fields = list(exp_rel_path="character"),
    methods = list(
      get_all_dirs = function()
      {
        exp_path = get_exp_path(exp_rel_path)
        # using the fact that every subexp has a config.json
        conf_path = get_file_paths_recursively(exp_path, 'config.json')
        subexp_dir = sapply(conf_path, get_dir_path)
        return(subexp_dir)
      },
      iter_each_subexp = function(subexp_analyzer)
      {
          subexp_dirs = get_all_dirs()
          results = list()
          for (subexp_dir in subexp_dirs)
          {
              analyzer = subexp_analyzer$new(subexppath=subexp_dir)
              ret = analyzer$run()
              results = append(results, list(ret))
          }
          return(results)
      }
    )
)


