from sqlalchemy import create_engine , text
import sqlalchemy as db


connection_string = "mysql+mysqlconnector://root:root@localhost:3306"
engine = create_engine(connection_string, echo=True)
print("connected .......")
with engine.connect() as conn:
    
    # 1.Database creation
    conn.execute(text("create database if not exists e_commerce"))
    print("database created ")
    
    # 2. Using Database
    
    conn.execute(text("USE e_commerce"))
    print("Using the Database e_commerce........")
    
    # 2. Tables creation
    
    conn.execute(text( """create table if not exists SUPPLIER(
                SUPP_ID int primary key, 
                SUPP_NAME varchar(50), 
                SUPP_CITY varchar(50), 
                SUPP_PHONE varchar(20)
                )""")
    )
    
                
    print("_____________Supplier Table Created_______________________")
    
    
    conn.execute(text( """create table if not exists CUSTOMER(
                CUS_ID INT NOT NULL, 
                CUS_NAME VARCHAR(20) NULL DEFAULT NULL, 
                CUS_PHONE VARCHAR(10), 
                CUS_CITY varchar(30) ,
                CUS_GENDER CHAR,
                PRIMARY KEY (CUS_ID))""")
    )
    
                
    print("_____________CUSTOMER Table Created_______________________")
    
    
    conn.execute(text( """create table if not exists CATEGORY(
                CAT_ID INT NOT NULL, CAT_NAME VARCHAR(20) NULL DEFAULT NULL,PRIMARY KEY (CAT_ID))""")
    )
    
                
    print("_____________Category Table Created_______________________")
    
    
    conn.execute(text( """create table if not exists PRODUCT(
                PRO_ID INT NOT NULL, PRO_NAME VARCHAR(20) NULL DEFAULT NULL, PRO_DESC VARCHAR(60) NULL DEFAULT NULL, CAT_ID INT NOT NULL,
                PRIMARY KEY (PRO_ID),FOREIGN KEY (CAT_ID) REFERENCES CATEGORY (CAT_ID))""")
    )
    
                
    print("_____________  Product Table Created_______________________")
    
    
    conn.execute(text( """create table if not exists PRODUCT_DETAILS(
                PROD_ID INT NOT NULL, PRO_ID INT NOT NULL, SUPP_ID INT NOT NULL, PROD_PRICE INT NOT NULL,
  PRIMARY KEY (PROD_ID),FOREIGN KEY (PRO_ID) REFERENCES PRODUCT (PRO_ID), FOREIGN KEY (SUPP_ID) REFERENCES SUPPLIER(SUPP_ID))""")
    )
    
                
    print("_____________Product Details Table Created_______________________")
    
    conn.execute(text( """create table if not exists ORDERS(
                ORD_ID INT NOT NULL, 
                ORD_AMOUNT INT NOT NULL, 
                ORD_DATE DATE, 
                CUS_ID INT NOT NULL, 
                PROD_ID INT NOT NULL,
                PRIMARY KEY (ORD_ID),
                FOREIGN KEY (CUS_ID) REFERENCES CUSTOMER(CUS_ID),
                FOREIGN KEY (PROD_ID) REFERENCES PRODUCT_DETAILS(PROD_ID))""")
    )
    
                
    print("_____________Order Table Created_______________________")
    
    
    conn.execute(text( """create table if not exists RATING(
                RAT_ID INT NOT NULL, CUS_ID INT NOT NULL, SUPP_ID INT NOT NULL, RAT_RATSTARS INT NOT NULL,PRIMARY KEY (RAT_ID),
                FOREIGN KEY (SUPP_ID) REFERENCES SUPPLIER (SUPP_ID),FOREIGN KEY (CUS_ID) REFERENCES CUSTOMER(CUS_ID))""")
    )
    
                
    print("_____________RATING Table Created_______________________")
    
    
    # Question 2
    
    delete_query=conn.execute(text(""" DELETE FROM SUPPLIER WHERE SUPP_ID IN (1,2,3,4,5)"""));
    
    print("deletion is done .... for SUPPLIER")
    
        # Inserting into SUPPLIER Table
    conn.execute(text(""" insert into SUPPLIER values 
                      (1,'Rajesh Retails','Delhi','1234567890'),
                      (2,'Appario Ltd.','Mumbai','258963147032'),
                      (3,'Knome products','Bangalore','9782315546'),
                      (4,'Bansal Retails','Kochi','8975463285'),
                      (5,'Mittal Ltd.','Lucknow','7898456532');"""
                      ));
    conn.commit()
    print("Insertion executed succesfully for SUPPLIER ...");
    
    delete_query=conn.execute(text(""" DELETE FROM CUSTOMER WHERE CUS_ID IN (1,2,3,4,5)"""));
    
    print("deletion is done .... for CUSTOMER")
    
        # Inserting into CUSTOMER Table
    conn.execute(text(""" insert into CUSTOMER values 
                      (1,'AAKASH','9999999999','DELHI','M'),
                      (2,'AMAN','9785463215','NOIDA','M'),
                      (3,'NEHA','9999999998','MUMBAI','F'),
                      (4,'MEGHA','9994562399','KOLKATA','F'),
                      (5,'PULKIT','7895999999','LUCKNOW','M');"""
                      ));
    conn.commit()
    print("Insertion executed succesfully for CUSTOMER ...");
    
    
   
    
    delete_query=conn.execute(text(""" DELETE FROM CATEGORY WHERE CAT_ID IN (1,2,3,4,5)"""));
    conn.commit()
    
    print("deletion is done .... for CATEGORY")
    
        # Inserting into CUSTOMER Table
    conn.execute(text(""" insert into CATEGORY values 
                      (1,'BOOKS'),
                      (2,'GAMES'),
                      (3,'GROCERIES'),
                      (4,'ELECTRONICS'),
                      (5,'CLOTHES');"""
                      ));
    conn.commit()
    print("Insertion executed succesfully for CATEGORY ...");
    
    #print("deletion is done .... for PRODUCT")
    
    delete_query=conn.execute(text(""" DELETE FROM PRODUCT WHERE PRO_ID IN (1,2,3,4,5)"""));
    conn.commit()
    
        # Inserting into CUSTOMER Table
    conn.execute(text(""" insert into PRODUCT values 
                      (1,'GTA V','DFJDJFDJFDJFDJFJF',2),
                      (2,'TSHIRT','DFDFJDFJDKFD',5),
                      (3,'ROG LAPTOP','DFNTTNTNTERND',4),
                      (4,'OATS','REURENTBTOTH',3),
                      (5,'HARRY POTTER','NBEMCTHTJTH',1);"""
                      ));
    conn.commit()
    print("Insertion executed succesfully for PRODUCT ...");
    
    
    
    
    
    
    
    delete_query=conn.execute(text(""" DELETE FROM PRODUCT_DETAILS WHERE PROD_ID IN (1,2,3,4,5) """));
    
    print("deletion is done .... for PRODUCT DETAILS")
    
        # Inserting into CUSTOMER Table
    conn.execute(text(""" insert into PRODUCT_DETAILS values 
                      (1,1,2,1500),
                      (2,3,5,30000),
                      (3,5,1,3000),
                      (4,2,3,2500),
                      (5,4,1,1000);"""
                      ));
    conn.commit()
    print("Insertion executed succesfully for PRODUCT DETAILS...");
    
    delete_query=conn.execute(text(""" DELETE FROM ORDERS WHERE ORD_ID IN (20,25,26,30,50) """));
    
    print("deletion is done .... for ORDERS")
    
        # Inserting into CUSTOMER Table
    conn.execute(text(""" insert into ORDERS values 
                      (20,1500,'2021-10-12',3,5),
                      (25,30500,'2021-09-16',5,2),
                      (26,2000,'2021-10-05',1,1),
                      (30,3500,'2021-08-16',4,3),
                      (50,2000,'2021-10-06',2,1);"""
                      ));
    conn.commit()
    print("Insertion executed succesfully for ORDERS...");
    
    
    delete_query=conn.execute(text(""" DELETE FROM RATING WHERE RAT_ID IN (1,2,3,4,5) """));
    
    print(" deletion is done .... for RATING")
    
    conn.execute(text(""" insert into RATING values 
                      (1,2,2,4),
                      (2,3,4,3),
                      (3,5,1,5),
                      (4,1,3,5),
                      (5,4,5,4)"""));
    conn.commit()
    
    
    #Question 3
   
    question_3=conn.execute(text(""" SELECT  CUS_GENDER,SUM(ORD_AMOUNT) from e_commerce.customer c inner join e_commerce.orders o
    on c.CUS_ID=o.CUS_ID where O.ORD_AMOUNT>=3000 GROUP BY CUS_GENDER ;"""));
    for row in question_3:
        print(str(row))
    
    #Question 4
    
    question_4= conn.execute(text(""" SELECT o.*,p.pro_name FROM e_commerce.orders o
    inner join e_commerce.product_details pd
    on o.prod_id=pd.prod_id inner join e_commerce.product p on pd.PRO_ID=p.PRO_ID
    inner join e_commerce.customer c on o.CUS_ID=c.CUS_ID where C.CUS_ID=2;"""))
    
    for row in question_4:
        print(list(row))
        
    # Question 5
    
    
    question_5=conn.execute(text(""" SELECT 	SUPP_ID,SUPP_NAME,SUPP_PHONE,SUPP_CITY FROM (
    SELECT S.*,pd.PRO_ID as PRO_ID,row_number() over(partition by SUPP_ID ORDER by SUPP_ID) as cnt FROM e_commerce.supplier s inner join e_commerce.product_details pd on s.SUPP_ID=pd.SUPP_ID
    ) as A where CNT>1;"""));
    
    for row in question_5:
        print(list(row))
        
    
    #Question 6
    question_6= conn.execute(text(""" SELECT  p.*,ORD_AMOUNT from e_commerce.product p inner join e_commerce.category c on p.CAT_ID=c.CAT_ID
    inner join e_commerce.product_details pd on p.PRO_ID=pd.PRO_ID
    inner join e_commerce.orders o on pd.PROD_ID=o.PROD_ID ORDER BY ORD_AMOUNT ASC limit 1;"""))
    
    for row in question_6:
        print(list(row))
    
        
    
    #Question 7
    
    question_7=conn.execute(text(""" SELECT p.PRO_ID,PRO_NAME FROM e_commerce.product p inner join e_commerce.product_details pd on p.pro_id=pd.pro_id
                inner join e_commerce.orders o on pd.PROD_ID=o.PROD_ID where ORD_DATE>'2021-10-05'"""));
                 
    for row in question_7:
        print(list(row))
    
    
    
    # Question 8
    question_8=conn.execute(text(""" SELECT s.SUPP_ID,s.SUPP_NAME,RAT_RATSTARS,CUS_NAME from e_commerce.supplier s 
inner join e_commerce.rating r on s.SUPP_ID=r.SUPP_ID
inner join e_commerce.customer C on r.CUS_ID=c.CUS_ID order BY RAT_RATSTARS DESC limit 3;"""))
    
    for row in question_8:
        print(list(row))
    
    

    question_9 =conn.execute(text(""" SELECT CUS_NAME,CUS_GENDER FROM e_commerce.customer where CUS_NAME LIKE 'A%' or CUS_NAME LIKE '%A'
    ;"""))
    
    for row in question_9:
        print(list(row))
        
    question_10=conn.execute(text(""" select CUS_GENDER,SUM(ORD_AMOUNT) FROM e_commerce.customer C INNER JOIN e_commerce.orders o on
c.CUS_ID=o.CUS_ID WHERE CUS_GENDER='M' group by CUS_GENDER;"""))
    
    for row in question_10:
        print(list(row))
        
    question_11=conn.execute(text(""" select C.* from e_commerce.customer c left outer join e_commerce.orders o on c.CUS_ID=o.CUS_ID;"""))

    for row in question_11:
        print(list(row))
        
    conn.close()

    
    
    
    
    
    
    
    
    

