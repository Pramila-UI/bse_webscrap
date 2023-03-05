
# import the module
from sqlalchemy import create_engine
from urllib.parse import quote_plus                    

# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root", 
                        pw=quote_plus("Master@123"),
                        db="Assignment"))

