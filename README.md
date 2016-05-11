<a href="http://dx.doi.org/10.5281/zenodo.13750"><img src="https://zenodo.org/badge/doi/10.5281/zenodo.13750.svg" alt="10.5281/zenodo.13750"></a>


Skeleton
========

Application Skeleton is a tool to generate skeleton applications --- easy-to-program, easy-to-run applications --- that mimic a real applications' parallel or distributed performance at a task (but not process) level.

Application classes that can be represented include: bag of tasks, map reduce, multi-stage workflow, and variations of these with a fixed number of iterations.

Applications are described as one or more stages.

Stages are described as one more more tasks.  Stages can also be iterative.

Tasks can be serial or parallel, and have compute and I/O (read and write) elements.

=======

Documentation about Skeletons can be found in the report directory

If you have questions, need support, want to report a bug, or want to request a feature, please do so by opening a new issue -- https://github.com/applicationskeleton/Skeleton/issues/new

=======

Contributors are welcome!

Ideally, these can be made via pull requests to the repository.

To receive email about commits to this repository, join the Google Groups "skeleton-commits" group,
http://groups.google.com/group/skeleton-commits/subscribe


=======

A paper about the first version of Application Skeletons is:
Z. Zhang and D. S. Katz, "Application Skeletons: Encapsulating MTC Application Task Computation and I/O," Proceedings of 6th Workshop on Many-Task Computing on Grids and Supercomputers (MTAGS), (in conjunction with SC13), 2013. http://dx.doi.org/10.1145/2503210.2503222

A paper about the current version is:
Z. Zhang and D. S. Katz, "Using Application Skeletons to Improve eScience Infrastructure," Proceedings of 10th IEEE International Conference on eScience, 2014.
http://dx.doi.org/10.1109/eScience.2014.9 (paper).
http://www.slideshare.net/danielskatz/using-application-skeletons-to-improve-escience-infrastructure (slides).
