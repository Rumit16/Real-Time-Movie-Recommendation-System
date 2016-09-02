'''
Created on Nov 30, 2015

@author: Rumit
'''
import sys
import timeit;
from pyspark import SparkContext
import numpy as np
from numpy.random import rand
from numpy import matrix

#def part(line):
#        values = [float(x) for x in line.split(',')]
#        return (values[0,:])
        
# function to calulate RMSE        
def rmse(user_rating, movies, users):
#    print user_rating
    mu = movies * users.T
#    print mu
    diff = user_rating - mu
#    print diff
    s_diff = (np.power(diff, 2)) / (movies_row * users_row)
    return np.sqrt(np.sum(s_diff))
    
def fix_user(x):  
    user = users_brod.value.T * users_brod.value
    for a in range(feature):
        user[a, a] = user[a,a] + lam * n

    new_user = users_brod.value.T * user_rating_brod.value[x,:].T
    return np.linalg.solve(user, new_user)

def fix_movie(x):
    ur = user_rating_brod.value.T
    movie = movies_brod.value.T * movies_brod.value
    for b in range(feature):
        movie[b, b] = movie[b,b] + lam * m
        
    new_movie = movies_brod.value.T * ur[x, :].T
    return np.linalg.solve(movie, new_movie)
   
if __name__ == "__main__":

    sc = SparkContext(appName="ALS_Final")
    
# Initializing the parameters iteration and Lambda
    lam = 0.001
    iteration =  10
    i = 0
    feature = 10
    rms = np.zeros(iteration)
    start = timeit.default_timer()
   
# Loading the movielens data in to the matrix
#   lines = sc.textFile("G:\\study\\Second Sem\\Cloud Computing\\Project\\Data\\ml-100k\\ml-100k\\full.csv")
    lines = sc.textFile(sys.argv[1])
    parts = lines.map(lambda l: l.split(","))
    
    user_rating = np.matrix(parts.collect()).astype('float')
#    user_rating = np.matrix('0 0 3.0 3.5 2.5 3.0;3.0 3.5 1.5 5.0 3.5 3.0;2.5 3.0 3.5 4.0 0 0;0 3.5 3.0 4.0 2.5 4.5;3.0 4.0 2.0 3.0 2.0 3.0;3.0 4.0 0 5.0 3.5 3.0;0 4.5 0 4.0 1.0 0 ')
    m,n = user_rating.shape
    
    user_rating_brod = sc.broadcast(user_rating)
# Initializing the weights matrix using the user_rating matrix
    w = np.zeros(shape=(m,n))
    for r in range(m):
        for j in range(n):
            if user_rating[r,j]>0.5:
                w[r,j] = 1.0
            else:
                w[r,j] = 0.0
      
# Randomly initialize movies and users matrix
    movies = matrix(rand(m, feature))
    movies_brod = sc.broadcast(movies)
    
    users = matrix(rand(n, feature))
    users_brod = sc.broadcast(users)
    
    movies_row,movies_col = movies.shape
    users_row,users_col = users.shape

# iterate until movies and users matrix converge
    while (i<iteration):
      
# solving movies matrix by keeping user matrix constant
        movies = sc.parallelize(range(movies_row)).map(fix_user).collect()
        movies_brod = sc.broadcast(matrix(np.array(movies)[:, :]))

# solving user matrix by keeping movie matrix constant 
        users = sc.parallelize(range(users_row)).map(fix_movie).collect()
        users_brod = sc.broadcast(matrix(np.array(users)[:, :]))

        error = rmse(user_rating, matrix(np.array(movies)), matrix(np.array(users)))
        rms[i] = error
        i = i+1
    
    fi_user = np.array(users).squeeze()
    fi_movie = np.array(movies).squeeze()
    final = np.dot(fi_movie,fi_user.T)
    
# subtracting the rating that user already rated
    rec_movie = np.argmax(final - 5 * w,axis =1)
    
# Predicting movies for each users
    for u in range(movies_row):
        r = rec_movie.item(u)
        p = final.item(u,r)
        print ('Prediction for user %d: Movie_id %d: Predicted rating %d' %(u+1,r+1,p) )
        
    print "RMSE after each iteration -",rms
    
    stop = timeit.default_timer()
    print "time-",stop - start
    print "Avg rmse-",np.mean(rms)
    sc.stop()

  

