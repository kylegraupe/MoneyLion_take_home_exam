import import_raw_to_db
import etl

if __name__ == "__main__":
    print("Hello, World!")

    import_raw_to_db.data_import_executive()

    etl.etl_executive()