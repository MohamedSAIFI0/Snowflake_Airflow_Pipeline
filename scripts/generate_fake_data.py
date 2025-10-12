from faker import Faker
import pandas as pd 
import random 

fake = Faker()

customers = [
    {"customer_id":i, "name":fake.name(), "country":fake.country(), "email":fake.email()}
    for i in range(1,101)
]
pd.DataFrame(customers).to_csv(r"/home/saifi/Desktop/eccomerce_project/data/customers.csv",index=False)



products = [
    {"product_id":i, "name":fake.word(),"category":random.choice(["Electronics","Clothing","Sports"]), "price":round(random.uniform(10,1000),2)}
    for i in range(1,21)
]
pd.DataFrame(products).to_csv(r"/home/saifi/Desktop/eccomerce_project/data/products.csv",index=False)



sales = [
    {"sale_id":i,"customer_id":random.randint(1,100),"product_id":random.randint(1,20),
    "quantity":random.randint(1, 5),"sale_date":fake.date_this_year()}
    for i in range(1, 501)
]
pd.DataFrame(sales).to_json(r"/home/saifi/Desktop/eccomerce_project/data/sales.json", index=False)