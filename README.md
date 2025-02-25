# v4d
A fastapi/uvicorn service with celery for async task scheduling/handling and redis as an in-memory key value store for storing job stats. 

Installation:

$ pip install fastapi celery redis uvicorn

$ docker run -d -p 6379:6379 redis

$ uvicorn main:app --host ec2-instance --port 8081 --reload

$ celery -A main.celery_app worker --loglevel=info

Running the service with the api calls as below:

1. Create a grid

   ![image](https://github.com/user-attachments/assets/a26718ae-2d81-46d3-b8a7-223fb8d3e8ac)

2. Grid status (with job status)

   ![image](https://github.com/user-attachments/assets/b7076e3d-99b5-47fa-8bde-ff167983a462)

3. Kill a job

   ![image](https://github.com/user-attachments/assets/336b51a1-3ba8-4fa3-a839-bd99a81cee9f)


