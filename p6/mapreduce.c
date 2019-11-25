/**
 * Author: Jiawei Gu
 * This is a mapreduce-like multithread library
 * It takes Mapper and Reducer function as input
 * and parse files concurrently
 *
 * Nov 22, 2019
 */

#include "mapreduce.h"
#include "param.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

/**
 * pair of key and value
 * linked list
 */
struct kv_pair {
    char* key;
    char* value;
    struct kv_pair* next;
};

/**
 * linked list of value
 *
 */
struct v_node {
    char* value;
    struct v_node* next;
};

/**
 * a cluster of values with the same key
 *
 */
struct k_cluster {
    char* key;
    struct v_node* v_cluster;
    struct k_cluster* next;
    struct v_node* curr_visit;
};

struct partition {
    struct kv_pair* pair;
    struct k_cluster* key_cluster;
    struct k_cluster* curr_k;
    pthread_mutex_t lock;
};

/**
 * a linked list of files storing filenames
 *
 */
struct file_node {
    char* file_name;
    struct file_node* next;
};

pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t reduce_lock = PTHREAD_MUTEX_INITIALIZER;
struct partition partition_table[TABLE_SIZE];  // hashmap of partitions
struct file_node* files;
Mapper map_function;
Reducer reduce_function;
Partitioner partition_function;
int num_par;
int next_job;

void merge_sort(struct kv_pair**, int, int);
void merge(struct kv_pair**, int, int, int);

/**
 * initialize locks and alloc memory for key and value strings
 * initialize Mapper, Reducer, Partitioner function pointers
 *
 */
void MR_init(int argc, char* argv[], Mapper map,
        Reducer reduce, Partitioner partition, int num_partitions) {
    pthread_mutex_init(&file_lock, NULL);
    pthread_mutex_init(&reduce_lock, NULL);

    files = NULL;
    // copy file names
    for (int i = 0; i < argc-1; i++) {
        struct file_node* temp = malloc(sizeof(struct file_node));
        if (temp == NULL) {
            perror("malloc");
        }
        temp->file_name = malloc(sizeof(char) * 16);
        if (temp->file_name == NULL) {
            perror("malloc");
        }
        strncpy(temp->file_name, argv[i+1], 16);
        temp->next = files;
        files = temp;
    }

    map_function = map;
    reduce_function = reduce;
    partition_function = partition;
    num_par = num_partitions;
    next_job = 0;

    // initialize partition's mutex lock to null
    for (int i = 0; i < TABLE_SIZE; i++) {
        pthread_mutex_init(&partition_table[i].lock, NULL);
        partition_table[i].pair = NULL;
        partition_table[i].key_cluster = NULL;
        partition_table[i].curr_k = NULL;
    }
}

/**
 * create a new key-value pair and store it to
 * the linked list
 */
void MR_Emit(char *key, char *value) {
    unsigned long partition_number = (*partition_function)(key, num_par);
    struct kv_pair *new = malloc(sizeof(struct kv_pair));
    if (new == NULL) {
        perror("malloc");
    }
    new->key = malloc(sizeof(char) * 16);
    if (new->key == NULL) {
        perror("malloc");
    }
    new->value = malloc(sizeof(char) * 16);
    if (new->value == NULL) {
        perror("malloc");
    }
    strncpy(new->key, key, 16);
    strncpy(new->value, value, 16);

    // acquire lock to add key value pair into partition assigned
    pthread_mutex_lock(&partition_table[partition_number].lock);
    new->next = partition_table[partition_number].pair;
    partition_table[partition_number].pair = new;
    // printf("pair %s-%s added\n", new->key, new->value);
    pthread_mutex_unlock(&partition_table[partition_number].lock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    int i = 0;
    while (num_partitions > 1) {
        i++;
        num_partitions /= 2;
    }
    i = 32 - i;

    // unsigned long number = 0;
    // number += (int) key[0] << 24;
    // number += (int) key[1] << 16;
    // number += (int) key[2] << 8;
    // number += (int) key[3];
    unsigned long number = strtoul(key, NULL, 0);
    return number >> i;
}

void* map_thread(void* arg) {
    while (1) {
        pthread_mutex_lock(&file_lock);
        if (files == NULL) {
            pthread_mutex_unlock(&file_lock);
            return NULL;
        }
        char* filename = files->file_name;
        struct file_node* temp = files;
        files = files->next;
        pthread_mutex_unlock(&file_lock);
        (*map_function)(filename);
        free(filename);
        free(temp);
        filename = NULL;
        temp = NULL;
    }
}

char* get_function(char* key, int partition_number) {
    struct k_cluster* curr_cluster = partition_table[partition_number].curr_k;

    while (curr_cluster != NULL) {
        if (strcmp(key, curr_cluster->key) != 0) {
            curr_cluster = curr_cluster->next;
            partition_table[partition_number].curr_k = curr_cluster;
        } else {
            if (curr_cluster->curr_visit != NULL) {
                char* value = curr_cluster->curr_visit->value;
                curr_cluster->curr_visit = curr_cluster->curr_visit->next;
                return value;
            } else {
                curr_cluster = curr_cluster->next;
                return NULL;
            }
        }
    }
    printf("key not found!\n");
    return NULL;
}

void* reduce_thread(void* arg) {
    // acquire lock to check next job
    pthread_mutex_lock(&reduce_lock);
    while (next_job < num_par) {
        int current_job = next_job;
        next_job++;
        pthread_mutex_unlock(&reduce_lock);
        pthread_mutex_lock(&partition_table[current_job].lock);

        struct partition* curr_partition = &partition_table[current_job];
        int num_pair = 0;
        struct kv_pair* temp = curr_partition->pair;
        while (temp != NULL) {
            num_pair++;
            temp = temp->next;
        }
        if (num_pair == 0) {
            pthread_mutex_unlock(&curr_partition->lock);
            pthread_mutex_lock(&reduce_lock);
            continue;
        }
        struct kv_pair** to_sort = malloc(sizeof(struct kv_pair*) * num_pair);
        if (to_sort == NULL) {
            perror("malloc");
        }
        int i = 0;
        while (curr_partition->pair != NULL) {
            to_sort[i] = curr_partition->pair;
            curr_partition->pair = curr_partition->pair->next;
            i++;
        }
        // time_t begin = clock();
        // use merge sort to sort the array
        merge_sort(to_sort, 0, num_pair-1);
        // time_t sort = clock();
        // printf("sort took: %f\n", (double)(sort - begin) / CLOCKS_PER_SEC);

        struct k_cluster* first_cluster = malloc(sizeof(struct k_cluster));
        if (first_cluster == NULL) {
            perror("malloc");
        }
        if (first_cluster == NULL) {
            perror("malloc");
        }

        // assign first key to the first kcluster
        first_cluster->key = to_sort[0]->key;
        // alloc first v cluster
        first_cluster->v_cluster = malloc(sizeof(struct v_node));
        if (first_cluster->v_cluster == NULL) {
            perror("malloc");
        }
        // assign value to the v node
        first_cluster->v_cluster->value = to_sort[0]->value;
        first_cluster->v_cluster->next = NULL;  // set v node's next to NULL.
        // set current visit to the first v node
        first_cluster->curr_visit = first_cluster->v_cluster;
        // set first key cluster's next to head
        first_cluster->next = NULL;
        // assign it to key cluster head
        curr_partition->key_cluster = first_cluster;
        curr_partition->curr_k = first_cluster;

        // keep track of the last key to determine
        // if necessary to create a new key cluster
        struct k_cluster* curr_k_cluster = first_cluster;

        struct k_cluster* ktail = first_cluster;
        // we add v_node to the tail because we want curr visit to be the head
        struct v_node* tail = first_cluster->v_cluster;

        // group kv_pairs together according to their key
        for (int a = 1; a < num_pair; a++) {
            if (strcmp(curr_k_cluster->key, to_sort[a]->key) == 0) {
                // create new v_node and add to the tail of v_cluster
                struct v_node* new_value = malloc(sizeof(struct v_node));
                if (new_value == NULL) {
                    perror("malloc");
                }
                new_value->value = to_sort[a]->value;
                new_value->next = NULL;
                tail->next = new_value;
                tail = new_value;

                // free key in the kv_pair since it loses reference here
                free(to_sort[a]->key);
                to_sort[a]->key = NULL;
            } else {
                // create a new k_cluster and assign v_cluster head
                struct k_cluster* new_cluster =
                    malloc(sizeof(struct k_cluster));
                if (new_cluster == NULL) {
                    perror("malloc");
                }
                new_cluster->key = to_sort[a]->key;
                new_cluster->v_cluster = malloc(sizeof(struct v_node));
                if (new_cluster->v_cluster == NULL) {
                    perror("malloc");
                }
                new_cluster->v_cluster->value = to_sort[a]->value;
                new_cluster->v_cluster->next = NULL;
                new_cluster->curr_visit = new_cluster->v_cluster;
                new_cluster->next = NULL;
                ktail->next = new_cluster;
                ktail = new_cluster;

                // update curr_k_cluster and tail
                curr_k_cluster = new_cluster;
                tail = new_cluster->v_cluster;
            }
        }
        // time_t cluster = clock();
        // printf("creating cluster took: %f\n",
        //  (double)(cluster - sort) / CLOCKS_PER_SEC);
        for (int x = 0; x < num_pair; x++) {
            free(to_sort[x]);
            to_sort[x] = NULL;
        }
        free(to_sort);
        to_sort = NULL;

        for (struct k_cluster* curr = partition_table[current_job].key_cluster;
                curr != NULL; curr = curr->next) {
            (*reduce_function)(curr->key, get_function, current_job);
        }

        struct k_cluster* kcurr = curr_partition->key_cluster;
        while (kcurr != NULL) {
            free(kcurr->key);
            kcurr->key = NULL;
            struct v_node* vcurr = kcurr->v_cluster;
            while (vcurr != NULL) {
                free(vcurr->value);
                vcurr->value = NULL;
                struct v_node* vtemp = vcurr;
                vcurr = vcurr->next;
                free(vtemp);
                vtemp = NULL;
            }
            struct k_cluster* ktemp = kcurr;
            kcurr = kcurr->next;
            free(ktemp);
            ktemp = NULL;
        }
        pthread_mutex_unlock(&curr_partition->lock);
        // acquire lock before checking next_job
        pthread_mutex_lock(&reduce_lock);
    }
    pthread_mutex_unlock(&reduce_lock);
    return NULL;
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Partitioner partition, int num_partitions) {
    MR_init(argc, argv, map, reduce, partition, num_partitions);

    // clock_t begin = clock();
    const int NUM_MAPPERS = num_mappers;
    const int NUM_REDUCERS = num_reducers;
    // creating num_mappers map_thread
    pthread_t map_pthreads[NUM_MAPPERS];
    for (int i = 0; i < NUM_MAPPERS; i++) {
        pthread_create(&map_pthreads[i], NULL, map_thread, NULL);
    }

    for (int i = 0; i < NUM_MAPPERS; i++) {
        pthread_join(map_pthreads[i], NULL);
    }

    // clock_t map_time = clock();
    // printf("Map threads took: %f\n",
    // (double)(map_time - begin) / CLOCKS_PER_SEC);
    free(files);
    files = NULL;

    pthread_t reduce_threads[NUM_REDUCERS];
    for (int i = 0; i < NUM_REDUCERS; i++) {
        pthread_create(&reduce_threads[i], NULL, reduce_thread, NULL);
    }

    for (int i = 0; i < NUM_REDUCERS; i++) {
        pthread_join(reduce_threads[i], NULL);
    }
    // clock_t reduce_time = clock();
    // printf("Reduce threads took: %f\n",
    // (double)(reduce_time - map_time) / CLOCKS_PER_SEC);
    return;
}

void merge_sort(struct kv_pair** array, int start, int end) {
    if (start < end) {
        int middle = (start + end) / 2;

        merge_sort(array, start, middle);
        merge_sort(array, middle+1, end);
        merge(array, start, middle, end);
    } else {
        return;
    }
}

/**
 * External source: https://www.geeksforgeeks.org/merge-sort/
 * paradigm of merge sort
 */
void merge(struct kv_pair** array, int start, int middle, int end) {
    int end_1 = middle - start + 1;
    int end_2 =  end - middle;

    // temp arrays to store left and right part
    struct kv_pair* left[end_1], *right[end_2];

    /* Copy data to temp arrays left[] and right[] */
    for (int i = 0; i < end_1; i++)
        left[i] = array[start + i];
    for (int j = 0; j < end_2; j++)
        right[j] = array[middle + 1 + j];

    /* Merge the temp arrays back into arr[left..right]*/
    int i = 0;  // Initial index of first subarray
    int j = 0;  // Initial index of second subarray
    int k = start;  // Initial index of merged subarray
    while (i < end_1 && j < end_2) {
        if (strcmp(left[i]->key, right[j]->key) <= 0) {
            array[k] = left[i];
            i++;
        } else {
            array[k] = right[j];
            j++;
        }
        k++;
    }

    while (i < end_1) {
        array[k] = left[i];
        i++;
        k++;
    }

    while (j < end_2) {
        array[k] = right[j];
        j++;
        k++;
    }
}
