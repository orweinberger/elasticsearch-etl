#elasticsearch-ETL

This is a WIP version of an ETL tool for Elasticsearch. It currently allows you to define an index you want to copy from and to and will run the `modifyResult` function on each document before pushing it back into the new index. This application uses the scan and scroll function to query for documents and the bulk API to insert documents back into the new index.

Pull requests and issues are welcomed.
