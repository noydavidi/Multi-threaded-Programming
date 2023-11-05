# Multi-threaded-Programming

**Background**

This project was created as a part of the Operating System course at The Hebrew University of Jerusalem. It is designed to showcase the functionality of a MapReduce framework and multi-threaded programming. The project consists of various components and threads that work together to process data and perform mapping and reducing tasks.

**Overview**

In this design, all threads, except for thread 0, go through three phases: Map, Sort, and Reduce. Thread 0, in addition to these three phases, also executes a special Shuffle phase. In the general case, where multiThreadLevel=n:
1.	Thread 0 runs Map, Sort, Shuffle, and then Reduce.
2.	Threads 1 through n-1 run Map, Sort, and then Reduce.
3.	The MapReduce framework is designed to handle synchronization challenges and efficiently process data.

**Features**

MapReduce Framework: The project includes a MapReduce framework that can perform data processing tasks in a parallel and distributed manner.
Multi-threading: It demonstrates the use of multi-threading to handle multiple tasks concurrently, improving performance.
Thread Synchronization: The code includes synchronization mechanisms like mutexes and barriers to ensure thread safety and proper coordination.


