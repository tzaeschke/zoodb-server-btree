
ZooDB Server B-Tree
===================

<a href="http://www.zoodb.org">
<img src="https://github.com/tzaeschke/zoodb/blob/master/doc/images/logo_510412_web.png" alt="ZooDB logo" align="right" />
</a>

This repository provides an alternative implementation of the server side B-Tree index.
It requires less space by using prefix sharing and scales better with large databases.

It was originally implemented by Jonas Nick and Bogdan Vancea.

This project is currently licensed under GPLv3 (GNU Public License), see file COPYING.

Copyright Jonas Nick, Bogdan Vancea and Tilmann Zäschke.


Dependencies
============
* ZooDB 0.5.1
* [JDO 3.1](https://db.apache.org/jdo/) (Java Data Objects): 
* [JTA](http://java.sun.com/products/jta/) (Java Transaction API):
* [JUnit](http://www.junit.org/) (currently use 4.12, but should work with newer and older versions as well):
* [Java 8](https://java.com/de/download/)
* [SLF4J](https://www.slf4j.org/) (Logging API)


Contact
=======
zoodb(AT)gmx(DOT)de

