import json, pprint, requests, textwrap, time


def wait_for_session_start(host_name, session_id):
    if session_id is None:
        raise ValueError("Session id not available")
    max_attempt_cnt = 20
    attempt_cnt = 0
    while attempt_cnt <= max_attempt_cnt:
        rsp = requests.get(host_name + '/sessions/' + str(session_id) + "/state")
        state = rsp.json()['state']
        if state in ['not_started', 'starting']:
            print("..")
            time.sleep(10)
        elif state in ['shutting_down', 'error', 'dead', 'success']:
            raise ValueError('Invalid session state: {}'.format(state))
        else:
            print("Session is in {} state.".format(state))
            break
        attempt_cnt += 1


def wait_for_statement_execution(statement_url):
    if statement_url is None:
        raise ValueError("statement_url id not available")
    max_attempt_cnt = 20
    attempt_cnt = 0
    while attempt_cnt <= max_attempt_cnt:
        rsp = requests.get(statement_url)
        state = rsp.json()['state']
        if state in ['running', 'waiting']:
            print("..")
            time.sleep(10)
        elif state in ['error', 'cancelling', 'cancelled']:
            raise ValueError('Statement is being cancelled or error out')
        else:
            print("Statement completed successfully.")
            break
        attempt_cnt += 1


host = 'http://10.10.72.78:8998'
data = {'kind': 'spark'}
headers = {'Content-Type': 'application/json'}
r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
print("Session id = {} ".format(r.json()['id']))

wait_for_session_start(host, r.json()['id'])

session_url = host + r.headers['location']
statements_url = session_url + '/statements'

data = {
    'code': textwrap.dedent("""
    val NUM_SAMPLES = 100000;
    val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
      val x = Math.random();
      val y = Math.random();
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _);
    println(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)
    """)
}

r = requests.post(statements_url, data=json.dumps(data), headers=headers)
pprint.pprint(r.json())

statement_url = host + r.headers['location']
print("statement url for monitoring: {}".format(statement_url))

wait_for_statement_execution(statement_url)

# Printing the results.
r = requests.get(statement_url, headers=headers)
pprint.pprint(r.json())

# Deleting the session
requests.delete(session_url, headers=headers)

