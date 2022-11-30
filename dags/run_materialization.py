from transform import MQLClient
import os
TRANSFORM_API_KEY = os.environ["TRANSFORM_API_KEY"]


def run_materialization():
    mql = MQLClient(TRANSFORM_API_KEY)
    mats = mql.list_materializations()
    success =0
    fail = 0
    for mat in mats:
        try:
            materialize_response= mql.materialize(mat.name, timeout=3600, start_time='2018-01-01')
            print(f"Successfully created table: {materialize_response.schema}.{materialize_response.table} ")
            success +=1
        except RuntimeError as err:
            print("Runtime error",err)
            fail +=1
        except Exception as err:
            print(f"Unexpected {err}, {type(err)}")
            fail += 1
    print(f"Successful materialization: {success}. Failed materialization: {fail} ")


run_materialization()


#Materialization Query Time
#What matrializations were succesfull
#What materializations failed

