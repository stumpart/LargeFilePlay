##Reading and Processing Large files

In  trying to read a large file. We injest the file data by using java Files stream.
Each file is placed on a Blocking queue with a set queue size.
There are multiple concurrent consumers that takes the lines from off the queue to do processing.
Instead of building all of this with our own blocking queues and executors,
we could easily leverage the Completion service API more concretely the ExecutorCompletionService,
however I find it too restrictive and hard to scale.

We carefully monitor the amount of memory used while at the same time doing a fast injestion of the data and concurrently
processing each line.