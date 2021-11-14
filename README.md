# LoadGenDynamoDB
DynamoDB Load generator

#### Usage

Edit command.properites

`````
dynamodb.awsAccessKey= *****
dynamodb.awsSecretKey= *****
dynamodb.command = I
dynamodb.prefix = KD
dynamodb.start = 0
dynamodb.end = 100
dynamodb.endpoint=https://dynamodb.ap-northeast-2.amazonaws.com
dynamodb.region=ap-northeast-2
dynamodb.table=customer
sleepmi=10
``````

awsAccessKey 와  awsSecretKey 를 입력   
dynamodb 에 생성 되는 데이터의 Key 는 {prefix}-### 형태로 생성 됩니다.   
Command 에 따라 I 인경우 Insert 가 진행 되면 U 인 경우 입력된 Key 정보 {prefix}-### 로 데이터를 조회 후 특정 컬럼을 변경 합니다.    
D 인 경우 Delete 가 진행 됩니다.
sleempmi 는 operation 종료 후 sleep 시간을 mi 로 지정 합니다. 

#### Java Operation


````
java -jar dynamo-loadgen.jar command.properties

````

