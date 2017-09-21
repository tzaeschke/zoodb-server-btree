
ZooDB
=====

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
* JDO 3.1 (Java Data Objects): 
  - URL: https://db.apache.org/jdo/
  - JAR: jdo2-api-3.1.jar
* JTA (Java Transaction API):
  - URL: http://java.sun.com/products/jta/
  - JAR: jta.jar
* JUnit (currently use 4.8.1, but should work with newer and older versions as well):
  - URL: http://www.junit.org/
  - JAR: junit-4.8.1.jar
* Java 8


Contact
=======
zoodb(AT)gmx(DOT)de

