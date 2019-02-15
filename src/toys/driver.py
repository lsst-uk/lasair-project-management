import toys
import time

toys.start_again()
print("---- Tables remade")

# This is the maximum number of objects 
n_objects = 1000

# Now make a lot of observations of the objects
t = time.time()
n_candidates = 10000
for i in range(n_candidates):
    toys.make_candidate(n_objects, debug=False)
print("---- %d candidates made in %.2f seconds" % (n_candidates, time.time()-t))
toys.print_numbers()

# Recompute the collective propoerties of each object
t = time.time()
toys.update_objects(debug=False)
print("---- objects freshened in %.2f seconds" % (time.time()-t))

# Now make more observations of the objects
t = time.time()
n_candidates = 10000
for i in range(n_candidates):
    toys.make_candidate(n_objects, debug=False)
print("---- %d candidates made in %.2f seconds" % (n_candidates, time.time()-t))
toys.print_numbers()

# Recompute the collective propoerties of each object
t = time.time()
toys.update_objects(debug=False)
print("---- objects freshened in %.2f seconds" % (time.time()-t))
