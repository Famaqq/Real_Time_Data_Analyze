This project is proposed by JIAN Xun, and implemented by JIAN Xun under the supervising of Prof. David Rossiter. If you have any ideas or suggestions about this project, please feel free to contact me (chineshboy@hotmail.com).

1. To use this project, firstly you should install and run your MySQL server (at least make sure you did this step before installing MySQL-python in step 4).

2. Please use the init.sql to create the database schema.

3. Then please modify the MySQL server configurations in the db_config.py (if your MySQL server is installed locally, usually you just need to change the USERNAME and PASSWORD)

4. Make sure that your Python version is 2.7 and following packages are installed:
    Flask
    SQLAlchemy
    MySQL-python
    numpy

5. Now you can run this project!

Tips:
    When installing MySQL-python, if you get an error of cannot find "mysql_config", please find the location of this file (usually in /usr/local/mysql/bin), and then add the location in your PATH environment variable, and then try again.