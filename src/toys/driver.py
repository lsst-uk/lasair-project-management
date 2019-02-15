import toys
import time

toys.start_again()
print("---- Tables remade")

# This is the maximum number of objects 
n_objects = 1000000

def addem(n_candidates):
    # Now make a lot of observations of the objects
    t = time.time()
    for i in range(n_candidates):
        toys.make_candidate(n_objects, debug=False)
    print("---- %d candidates made in %.2f seconds" % (n_candidates, time.time()-t))
    toys.print_numbers()

    # Recompute the collective properties of each object
    t = time.time()
    toys.update_objects(debug=False)
    print("---- objects freshened in %.2f seconds" % (time.time()-t))

addem(10000)
addem(1000)
addem(10000)
