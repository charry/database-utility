database-utility
================

* Why this lib
Hibernate is too heavy for me, even MyBatis is too heavy for me, so that's the reason why I created this one. I try to get rid of most of the configurations,
but this leads to another problem, some rules must be followed if you need this lib.

* Exception Handling
You might notice that almost all exceptons are caught internally, I do this intentionally, the purpose is to make it easy for client code/developer to call. 
So in some special scenario, this might cause problem, if you really need to catch exceptions, please update the code at will. I'll try to figure out a way
to strike a balance.

* Remark
This is just a draft of readme, I'll add more information later.