import redis
import random
import string

from concurrent.futures import ThreadPoolExecutor


r = redis.Redis()

def sample_data():
    '''Creation of sample data in redis'''

    r.rpush('links', *["https://sampletesting.com/?authuser=" + ''.join((random.sample(string.ascii_lowercase, 10))) for _ in range(10**6)])
    print("'Links' created and data added successfully!!!")


def batching(list, size):
    '''
    Batch for processing redis list 
    
        Parameters:
            list () : the redis list to be processed
            size (int) : size of the batch

        Returns:
            Creates list to be added to the Redis set
    '''

    list_length = r.llen(list)
    for ndx in range(0, list_length, size):
        yield r.lrange(list, ndx, min(ndx + size, list_length))


def set_add(link):
    '''
    Function for adding items in the set
    
            Parameters:
                link : data to be processed
            
            Returns:
                Adds the the data to redis set
    '''

    r.sadd("sets_links", *list(link))


if __name__ == "__main__":

    # Main Function

    sample_data()

    with ThreadPoolExecutor() as executor:
        results = [executor.submit(set_add, link) for link in batching('links', 10**4)]
