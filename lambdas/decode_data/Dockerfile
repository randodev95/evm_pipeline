FROM public.ecr.aws/lambda/python:3.11

# Copy shared utilities first
COPY shared/ ${LAMBDA_TASK_ROOT}/shared/

# Copy requirements and install dependencies
COPY decode_data/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

# Copy handler
COPY decode_data/handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.handler"]
