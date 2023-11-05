#include "MapReduceFramework.h"
#include "Barrier.cpp"
#include "iostream"
#include "Barrier.h"
#include <atomic>
#include <algorithm>

#define SYS_ERROR "system error: "
#define SHIFT_TO_STAGE 62
#define SHIFT_TO_PROCESSED_NUM 31
#define LAST_31_BITS 0x7fffffff
#define EXIT_FAIL 1

using namespace std;

/*
 * Declaring JobContext for using it in Context struct
 */
struct JobContext;

/*
 * A wrapper for JobContext, includes id and the vector.
 */
struct Context {
  int id;
  JobContext* job_context {};
  IntermediateVec intermediate_vec {};
};

/*
 * A struct for grouping the mutexes
 */
struct Mutexes {
    pthread_mutex_t start_map_and_reduce_mutex {};
    pthread_mutex_t reduce_mutex {};
    pthread_mutex_t wait_mutex {};
    pthread_mutex_t emit2_mutex {};
    pthread_mutex_t emit3_mutex {};
};

/*
 *
 */
struct JobContext {
    const MapReduceClient* client{};
    const InputVec* input_vec{};
    OutputVec* output_vec{};
    int multi_thread_level = 0;
    Mutexes* mutexes{};
    pthread_t* threads{};
    Context* contexts{};
    atomic <uint64_t> counter{0};
    atomic <uint64_t> intermediary_counter{0};
    vector<IntermediateVec> que{};
    bool is_job_waiting = false;
    Barrier* barrier{};
};

/*
 * Lock the mutex
 */
void lock_mutex(pthread_mutex_t* mutex) {
  if(pthread_mutex_lock(mutex) != 0) {
      cout << SYS_ERROR << "mutex lock failed" << endl;
      exit(EXIT_FAIL);
    }
}

/*
 * Unlock the mutex
 */
void unlock_mutex(pthread_mutex_t* mutex) {
  if(pthread_mutex_unlock(mutex) != 0) {
      cout << SYS_ERROR << "mutex unlock failed" << endl;
      exit(EXIT_FAIL);
    }
}

/*
 * Initialize the mutex
 */
void init_mutex(pthread_mutex_t* mutex) {
  if(pthread_mutex_init(mutex, nullptr) != 0) {
      cout << SYS_ERROR << "mutex init failed" << endl;
      exit(EXIT_FAIL);
    }
}

/*
 * Destroy the mutex
 */
void destroy_mutex(pthread_mutex_t* mutex){
  if(pthread_mutex_destroy(mutex) != 0) {
      cout << SYS_ERROR << "mutex destroy failed" << endl;
      exit(EXIT_FAIL);
    }
}

/*
 * Return true if the first key is smaller than the second key. Used in the sort phase
 */
bool compare_intermediate_pairs(IntermediatePair a, IntermediatePair b) {
  return *a.first < *b.first;
}

/*
 * Get the maximum key from a intermediate vector of a JobContext. Used in shuffle phase
 */
K2* get_max_key_from_context_vector(JobContext* job_context) {
  K2* max = nullptr;

  for (int i=0; i < job_context->multi_thread_level; i++){
    auto context_vec = job_context->contexts[i].intermediate_vec;
    if(context_vec.empty()){
        continue;
    }

    K2* curr_key = job_context->contexts[i].intermediate_vec.back().first;
    if (max == nullptr) {
      max = curr_key;
      continue;
    }

    if (*max < *curr_key) {
      max = curr_key;
    }
  }
  return max;
}

/*
 * Check if two keys are equal using just the operator <, used in shuffle phase
 */
bool compare_keys (K2* k1, K2* k2) {
  return (!(*k1 < *k2) and !(*k2 < *k1));
}

/*
 * This function produces a (K2*, V2*) pair.
 * The function receives as input intermediary element (K2, V2) and context
 * which contains data structure of the thread that created the intermediary
 * element. The function saves the intermediary element in the context data
 * structures. In addition, the function updates the number of intermediary
 * elements using atomic counter.
 * Please pay attention that emit2 is called from the client's map function and
 * the context is passed from the framework to the client's map function as
 * parameter.
 */
void emit2 (K2* key, V2* value, void* context) {
  auto full_context = (Context*) context;
  lock_mutex(&full_context->job_context->mutexes->emit2_mutex);
  full_context->intermediate_vec.push_back({key, value});
  full_context->job_context->intermediary_counter += 1;
  unlock_mutex(&full_context->job_context->mutexes->emit2_mutex);
}

/*
 * This function produces a (K3*, V3*) pair.
 * The function receives as input output element (K3, V3) and context which
 * contains data structure of the thread that created the output element. The
 * function saves the output element in the context data structures (output
 * vector). In addition, the function updates the number of output elements
 * using atomic counter.
 * Please pay attention that emit3 is called from the client's reduce function
 * and the context is passed from the framework to the client's map function
 * as parameter.
 */
void emit3 (K3* key, V3* value, void* context) {
  auto job_context = (JobContext*) context;
  lock_mutex(&job_context->mutexes->emit3_mutex);
  job_context->output_vec->push_back ({key, value});
  unlock_mutex(&job_context->mutexes->emit3_mutex);
}

/*
 * The map phase in the framework.
 */
void map_phase(Context* context) {
    auto job_context = context->job_context;
    job_context->counter |= (1LL << SHIFT_TO_STAGE);
    uint64_t old_value = (job_context->counter++) & LAST_31_BITS;
    while (old_value < context->job_context->input_vec->size()) {
      auto pair = (*job_context->input_vec)[old_value];
      job_context->client->map(pair.first, pair.second, context);
      job_context->counter += 1LL << SHIFT_TO_PROCESSED_NUM;
      old_value = (context->job_context->counter++) & LAST_31_BITS;
    }
}

/*
 * The sort phase in the framework
 */
void sort_phase(Context* context){
  sort(context->intermediate_vec.begin(), context->intermediate_vec.end(), compare_intermediate_pairs);
}

/*
 * The shuffle phase in the framework. Only when thread's id is 0
 */
void shuffle_phase(Context* context){
  auto job_context = context->job_context;
  job_context->counter = (2LL << 62) + (job_context->intermediary_counter << 31);
  K2* max = get_max_key_from_context_vector(job_context);

  while(max != nullptr) {
    auto curr = IntermediateVec{};
    for(int i =0; i < job_context->multi_thread_level; i++) {
      auto context_vec = &job_context->contexts[i].intermediate_vec;
      while (!context_vec->empty() and compare_keys(context_vec->back().first, max)) {
        job_context->counter++;
        curr.push_back(context_vec->back());
        context_vec->pop_back();
      }
    }

    job_context->que.push_back(curr);
    max = get_max_key_from_context_vector(job_context);
  }
}

void reduce_phase(Context* context) {
  auto job_context = context->job_context;
  lock_mutex(&job_context->mutexes->reduce_mutex);
  while(!job_context->que.empty()) {
      auto curr = job_context->que.back();
      job_context->que.pop_back();
      job_context->client->reduce(&curr, job_context);
      job_context->counter += curr.size();
    }
  unlock_mutex(&job_context->mutexes->reduce_mutex);
}

/*
 * Starts the framework cycle using all the phases: map, sort, shuffle and reduce. y
 */
void* framework_cycle(void* argument) {
  auto full_context = (Context*) argument;
  auto job_context = full_context->job_context;

  map_phase(full_context);
  sort_phase(full_context);

  job_context->barrier->barrier();

  if(full_context->id == 0) {
    shuffle_phase(full_context);
    job_context->counter = (3LL << SHIFT_TO_STAGE) +
        (job_context->intermediary_counter << SHIFT_TO_PROCESSED_NUM);
  }

  job_context->barrier->barrier();
  reduce_phase(full_context);
  return nullptr;
}

/*
 * This function starts running the MapReduce algorithm (with several threads)
 * and returns a JobHandle
 * client – The implementation of MapReduceClient or in other words the task
 * that the framework should run.
 * inputVec – a vector of type std::vector<std::pair<K1*, V1*>>, the input
 * elements.
 * outputVec – a vector of type std::vector<std::pair<K3*, V3*>>, to which the
 * output elements will be added before returning. You can assume that
 * outputVec is empty.
 * multiThreadLevel – the number of worker threads to be used for running the
 * algorithm. You will have to create threads using c function pthread_create.
 * You can assume multiThreadLevel argument is valid (greater or equal to 1).
 * Returns - The function returns JobHandle that will be used for monitoring
 * the job.
 * You can assume that the input to this function is valid.
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
  auto job_context = new JobContext {};
  job_context->client = &client;
  job_context->input_vec = &inputVec;
  job_context->output_vec = &outputVec;
  job_context->multi_thread_level = multiThreadLevel;
  job_context->mutexes = new Mutexes {};
  job_context->barrier = new Barrier(multiThreadLevel);

  init_mutex(&job_context->mutexes->start_map_and_reduce_mutex);
  init_mutex(&job_context->mutexes->reduce_mutex);
  init_mutex(&job_context->mutexes->wait_mutex);
  init_mutex (&job_context->mutexes->emit2_mutex);
  init_mutex(&job_context->mutexes->emit3_mutex);

  job_context->threads = new pthread_t[multiThreadLevel];
  job_context->contexts = new Context[multiThreadLevel];

  lock_mutex(&job_context->mutexes->start_map_and_reduce_mutex);

  for(int i=0; i < multiThreadLevel; i++) {
    job_context->contexts[i].id = i;
    job_context->contexts[i].job_context = job_context;
    if(pthread_create (&job_context->threads[i], nullptr,
                       framework_cycle, &job_context->contexts[i]) != 0) {
      cout << SYS_ERROR << "pthread_create failed" << endl;
      exit(EXIT_FAIL);
    }
  }

  unlock_mutex(&job_context->mutexes->start_map_and_reduce_mutex);

  return job_context;
}

/*
 * A function gets JobHandle returned by startMapReduceFramework and waits
 * until it is finished.
 * Hint – you should use the c function pthread_join.
 * It is legal to call the function more than once and you should handle it.
 * Pay attention that calling pthread_join twice from the same process has
 * undefined behavior and you must avoid that.
 */
void waitForJob(JobHandle job) {
  auto job_context = (JobContext*) job;
  lock_mutex(&job_context->mutexes->wait_mutex);
  if(job_context->is_job_waiting) {
    unlock_mutex(&job_context->mutexes->wait_mutex);
    return;
  }

  job_context->is_job_waiting = true;

  for(int i=0; i < job_context->multi_thread_level; i++) {
    if(pthread_join(job_context->threads[i], nullptr) != 0) {
      cout << SYS_ERROR << "pthread_join failed" << endl;
      exit(EXIT_FAIL);
    }
  }

  unlock_mutex(&job_context->mutexes->wait_mutex);
}

/*
 * This function gets a JobHandle and updates the state of the job into the
 * given JobState struct
 */
void getJobState(JobHandle job, JobState* state) {
  auto job_context = (JobContext*) job;
  auto counter_val = job_context->counter.load();

  // shift the counter_val to the stage number (first 2 bits):
  state->stage = (stage_t) (counter_val >> SHIFT_TO_STAGE);
  float p = 0;
  switch(state->stage) {
      case UNDEFINED_STAGE:
        break;
      case MAP_STAGE:
        p = ((float)((counter_val >> SHIFT_TO_PROCESSED_NUM) & LAST_31_BITS)) /
            (float)(job_context->input_vec->size()) * 100;
        break;
      default:
        p = ((float)(counter_val & LAST_31_BITS) /
             (float)((counter_val >> SHIFT_TO_PROCESSED_NUM) & LAST_31_BITS)) * 100;
        break;
  }
  state->percentage = p;
}

/*
 * Releasing all resources of a job. You should prevent releasing resources
 * before the job finished. After this function is called the job handle will
 * be invalid.
 * In case that the function is called and the job is not finished yet wait
 * until the job is finished to close it.
 * In order to release mutexes and semaphores (pthread_mutex, sem_t) you should
 * use the functions pthread_mutex_destroy, sem_destroy.
 */
void closeJobHandle(JobHandle job) {
  auto job_context = (JobContext*) job;
  waitForJob(job);

  delete[] job_context->contexts;
  delete[] job_context->threads;

  destroy_mutex(&job_context->mutexes->wait_mutex);
  destroy_mutex(&job_context->mutexes->emit2_mutex);
  destroy_mutex(&job_context->mutexes->emit3_mutex);
  destroy_mutex(&job_context->mutexes->reduce_mutex);
  destroy_mutex(&job_context->mutexes->start_map_and_reduce_mutex);

  delete job_context->mutexes;
  delete job_context->barrier;

  delete job_context;
}
