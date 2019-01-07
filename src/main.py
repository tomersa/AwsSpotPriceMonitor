import time
import boto3
import paramiko
import threading

INSTANCE_FILTER = {'Name': 'instance-state-name', 'Values': ['running']}


def main():
    spot_price_monitor = SpotPriceMonitor()
    vm_communication = VmCommunication()

    spot_price_monitor.start()
    vm_communication.start()

    spot_price_monitor.join()
    vm_communication.join()


class Flags:
    _instance = None

    def __init__(self):
        self._flags = {'above_threshold_signal': False, 'rerun_vms_signal': False}

    @staticmethod
    def getFlags():
        if not Flags._instance:
            Flags._instance = Flags()

        return dict(Flags._instance._flags)

    @staticmethod
    def setFlag(flag):
        Flags._instance._flags[flag] = True
        print "[Flags] - %s was set" % flag

    def unsetFlag(self, flag):
        Flags._instance._flags[flag] = False
        print "[Flags] - %s was unset" % flag


class SpotPriceMonitor(threading.Thread):
    _THRESHOLD = 0.7
    _THRESHOLD_DEADLINE = 5  # If threshold is passed more than this number of second it's time to start/stop the vms

    def __init__(self):
        super( SpotPriceMonitor, self).__init__()
        self._above_threshold_time = None
        self._client = getEc2Client()

    def run(self):
        while True:
            current_price = self._get_spot_prices()
            if current_price > SpotPriceMonitor._THRESHOLD:
                if not self._above_threshold_time:
                    self._above_threshold_time = time.time()
                else:
                    if time.time() - self._above_threshold_time > SpotPriceMonitor._THRESHOLD_DEADLINE:
                        Flags.setFlag('above_threshold_signal')

            else:
                self._above_threshold_time = None
                Flags.unsetFlag('above_threshold_signal')
                Flags.setFlag('rerun_vms_signal')

            time.sleep(1)

    def _get_spot_prices(self):
        prices = self._client.describe_spot_price_history(InstanceTypes=['t2.micro'], MaxResults=1, AvailabilityZone='us-east-1a')
        return prices['SpotPriceHistory'][0]['SpotPrice']


class VmCommunication(threading.Thread):
    _VM_1_QUEUE_NAME = "TestQueue"  # The queue name used for testing
    _VMS_THRESHOLD = 1  # Below this threshold we should stop shutting down VMs

    def __init__(self):
        super(VmCommunication, self).__init__()
        self._test_queue = boto3.resource('sqs').get_queue_by_name(QueueName=VmCommunication._VM_1_QUEUE_NAME)
        self._sent_shutdown_signal = False
        self._sent_rerun_signal = False
        self._ec2 = getEc2Client()

    def run(self):
        if Flags.getFlags()['above_threshold_signal']:
            if not self._sent_shutdown_signal:
                self.test_queue_message("[VMCommunication] Start shutting down vms")
                self._sent_shutdown_signal = True
                self._sent_rerun_signal = False

        if Flags.getFlags()['rerun_vms_signal']:
            if not self._sent_rerun_signal:
                self.test_queue_message("[VMCommunication] Rerun VMs")
                self._sent_rerun_signal = True
                self._sent_shutdown_signal = False

        time.sleep(1)

    def test_queue_message(self, msg):
        self._test_queue.send_message(MessageBody=msg)
        print "queue %s received the following message: %s" % (self._test_queue.attributes['QueueArn'], msg)

    def _get_number_of_running_vms(self):
        return len(self._ec2.instances().filter(INSTANCE_FILTER))

#From here it's just for pocs I havn't got to the point of integrating this to the code.
def getListOfProcesses():
    def get_list_of_process_from_machine(instance):
        cmd = "ps -ef"  # Assuming: Working on a linux machine
        return sshToMachine(instance, cmd, instance_filter=INSTANCE_FILTER)
    executeOnListOfMachinesPeriodically(get_list_of_process_from_machine)

def sshToMachine(instance, cmd):
    key = paramiko.RSAKey.from_private_key_file("../res/Test.pem")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect/ssh to an instance
    # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
    client.connect(hostname=instance.public_dns_name, username="ec2-user", pkey=key)

    # Execute a command(cmd) after connecting/ssh to an instance
    stdin, stdout, stderr = client.exec_command(cmd)
    response = stdout.read()

    # close the client connection once the job is done
    client.close()
    return response

def executeOnListOfMachinesPeriodically(cmd = lambda instance: "State: %s,\tId: %s" % (instance.state['Name'], instance.id), instance_filter= []):
    ec2 = boto3.resource('ec2')
    while True:
        instances = ec2.instances.filter(instance_filter)
        for i in instances:
            print cmd(i)
        print ""
        time.sleep(3)

def getEc2Client():
    client = boto3.client('ec2', region_name='us-east-1')
    return client


if __name__ == "__main__":
    print main()