This code is a toy version of the candidate-object model of ZTF transients.

Each candidate (detection) from ZTF has an "objectId", defining the
astrophysical object that has been detected. All the candidates with a given
objectId comprise a history of that object, with its light curve etc.

In this toy model, each candidate has a number called "payload" that is a 
random float in [0,1). We would like to build in each object a quantity called
"payloadMax" which is the maximum of all the payload values of all the 
candidates belonging to that object.

We assume that there are only "n_objects" in total, so each candidate is 
assigned to one of them.

The code "toys.py" defines the functions used in the code "driver.py".
They are:

-- start_again()
This function drops both the candidate and object table, then rebuilds 
them. The SQL CREATE TABLE statements are in the toys.py code, so the
tables can be modified for efficiency if wanted.

-- make_candidate(n_objects, debug=False)
This function adds a new candidate to the candidates table, selecting a random 
object, and a random payload. It also updates the objects table by setting the 
associated object to "stale=1". The update_objects function will look for 
these to recompute the "payloadMax".

-- update_objects(debug=False)
For all the objects that are stale, get the list of associated candidates
and send these to the "freshen_object" function. In this case, the freshening
is simply to recompute the max of the payload numbers of the candidates.

-- print_numbers()
Prints out the number of candidates and number of objects.

Output
---- Tables remade
---- 25000000 candidates made in 38087.86 seconds
  25000000 candidates and 1000000 objects
---- objects freshened in 8120.85 seconds
---- 10 candidates made in 0.04 seconds
  25000010 candidates and 1000000 objects
---- objects freshened in 148.16 seconds
---- 100 candidates made in 0.16 seconds
  25000110 candidates and 1000000 objects
---- objects freshened in 148.72 seconds
---- 1000 candidates made in 1.56 seconds
  25001110 candidates and 1000000 objects
---- objects freshened in 148.03 seconds
---- 10000 candidates made in 15.43 seconds
  25011110 candidates and 1000000 objects
---- objects freshened in 167.69 seconds
---- 100000 candidates made in 150.87 seconds
  25111110 candidates and 1000000 objects
---- objects freshened in 791.10 seconds

