import time
import boto3
import paramiko
import threading

USERNAME_ON_INSTANCE = "ec2-user"

PEM_FILE_LOCATION = "../res/Test.pem"
RUNNING_INSTANCES_FILTER = {'Name': 'instance-state-name', 'Values': ['running']}


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
    def get_flags():
        if not Flags._instance:
            Flags._instance = Flags()

        return dict(Flags._instance._flags)

    @staticmethod
    def set_flag(flag):
        Flags._instance._flags[flag] = True
        print "[Flags] - %s was set" % flag

    def unset_flag(self, flag):
        Flags._instance._flags[flag] = False
        print "[Flags] - %s was unset" % flag


class SpotPriceMonitor(threading.Thread):
    _THRESHOLD = 0.7
    _THRESHOLD_DEADLINE = 5  # If threshold is passed more than this number of second it's time to start/stop the vms

    def __init__(self):
        super(SpotPriceMonitor, self).__init__()
        self._above_threshold_time = None
        self._client = get_ec2_client()

    def run(self):
        while True:
            current_price = self._get_spot_prices()
            if current_price > SpotPriceMonitor._THRESHOLD:
                if not self._above_threshold_time:
                    self._above_threshold_time = time.time()
                else:
                    if time.time() - self._above_threshold_time > SpotPriceMonitor._THRESHOLD_DEADLINE:
                        Flags.set_flag('above_threshold_signal')
                        Flags.unset_flag('rerun_vms_signal')

            else:
                self._above_threshold_time = None
                Flags.unset_flag('above_threshold_signal')
                Flags.set_flag('rerun_vms_signal')

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
        self._ec2 = get_ec2_client()

    def run(self):
        while True:
            if Flags.get_flags()['above_threshold_signal']:
                if not self._sent_shutdown_signal:
                    self._test_queue_message("[VMCommunication] Start shutting down vms")
                    self._sent_shutdown_signal = True
                    self._sent_rerun_signal = False

            if Flags.get_flags()['rerun_vms_signal']:
                if not self._sent_rerun_signal:
                    self._test_queue_message("[VMCommunication] Rerun VMs")
                    self._sent_rerun_signal = True
                    self._sent_shutdown_signal = False

            time.sleep(1)

    def _test_queue_message(self, msg):
        self._test_queue.send_message(MessageBody=msg)
        print "queue %s received the following message: %s" % (self._test_queue.attributes['QueueArn'], msg)

    def _get_number_of_running_vms(self):
        return len(self._ec2.instances().filter(RUNNING_INSTANCES_FILTER))


# From here it's just for POCs I haven't got to the point of integrating this to the code.
def get_list_of_process_from_machine(instance):
    cmd = "ps -ef"  # Assuming: Working on a linux machine
    return execute_command_on_instance(instance, cmd, instance_filter=RUNNING_INSTANCES_FILTER)


def get_list_of_processes():
    execute_command_on_list_of_machines_periodically(get_list_of_process_from_machine)


def execute_command_on_instance(instance, cmd):
    key = paramiko.RSAKey.from_private_key_file(PEM_FILE_LOCATION)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect/ssh to an instance
    client.connect(hostname=instance.public_dns_name, username=USERNAME_ON_INSTANCE, pkey=key)

    # Execute a command(cmd) after connecting/ssh to an instance
    stdin, stdout, stderr = client.exec_command(cmd)
    response = stdout.read()

    # close the client connection once the job is done
    client.close()
    return response


def execute_command_on_list_of_machines_periodically(cmd=lambda instance: "State: %s,\tId: %s" %
                                                                          (instance.state['Name'], instance.id),
                                                     instance_filter=[]):
    ec2 = boto3.resource('ec2')
    while True:
        instances = ec2.instances.filter(instance_filter)
        for i in instances:
            print cmd(i)
        print ""
        time.sleep(3)


def get_ec2_client():
    client = boto3.client('ec2', region_name='us-east-1')
    return client


if __name__ == "__main__":
    print main()
