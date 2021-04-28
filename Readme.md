# MIT6.824-Lab(2020)
homepage： http://nil.lcs.mit.edu/6.824/2020/index.html

It should be able to pass all tests reliably except 
```
Test: shard deletion (challenge 1) 
```

I found in lab4 the test script will not `KILL()` master server after each test, 
which will make master server print out debug logs forever.
