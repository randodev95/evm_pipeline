FROM public.ecr.aws/lambda/python:3.11

# Copy shared utilities first
COPY shared/ ${LAMBDA_TASK_ROOT}/shared/

# Copy requirements and install dependencies
COPY fetch_latest_block/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

# Copy handler
COPY fetch_latest_block/handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.handler"]
