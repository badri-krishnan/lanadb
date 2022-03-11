This is the version of a attempt at creating a key value store to learn rust.
Learning rust right now from the beginning is difficult but projects like this are great way to review fundamental concepts and write them in a different language. The name of this project will be called LanaDB.

Functional Requirements:
    1) Create A Value for a Key
    2) Get a Value for  Key
    3) Update a Value for a  Key
    4) Delete a Value for a Key

Nonfunctional Requirements:
    1) Try to make everything fit in memory when possible
    2) Once memory is file figure out an efficient way to store on file and do lookups(Log Structured Merge Tree)
    3) Offer a GRPC and HTTP Based Api to make calls for Create, Update, Delete


