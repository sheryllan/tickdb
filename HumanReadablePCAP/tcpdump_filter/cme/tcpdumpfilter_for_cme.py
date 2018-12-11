import sys, getopt, os, re
import subprocess
import xml.dom.minidom
import time

def add_filter_string(filter_str, ip, port, srcOrDst):
    if filter_str.strip():
        filter_str += " or "
    if srcOrDst == "dst":
        filter_str += "(dst host {} and dst port {})".format(ip, port)
    elif srcOrDst == "src":
        filter_str += "(host {} and port {})".format(ip, port)
    return filter_str

def parse_mdp3_config(config_file):
    config = {}
    DOMTree = xml.dom.minidom.parse(config_file)
    mdp3_root_element = DOMTree.documentElement
    channels = mdp3_root_element.getElementsByTagName("channel")
    for channel in channels:
        #just for debug
        #if channel.hasAttribute("label") and channel.hasAttribute("id"):
        #    print("channel {} - {}".format(channel.getAttribute("label"), channel.getAttribute("id")))
        #just for debug
        channel_filter_string =""
        channel_connections = channel.getElementsByTagName("connections")
        connections = channel_connections[0].getElementsByTagName("connection")
        for channel_connection in connections:
            #print("   connection ----- {} ".format(channel_connection.getAttribute("id")))
            protocol_type = channel_connection.getElementsByTagName('protocol')[0].childNodes[0].data
            if protocol_type == "TCP/IP":
                ip = channel_connection.getElementsByTagName('host-ip')[0].childNodes[0].data
                port = channel_connection.getElementsByTagName('port')[0].childNodes[0].data
                #print("       TCP {}:{}".format(ip,port))
                channel_filter_string = add_filter_string(channel_filter_string, ip, port, 'src')
            elif protocol_type == "UDP/IP":
                ip = channel_connection.getElementsByTagName('ip')[0].childNodes[0].data
                port = channel_connection.getElementsByTagName('port')[0].childNodes[0].data
                #print("       UDP {}:{}".format(ip,port))
                channel_filter_string = add_filter_string(channel_filter_string, ip, port, 'dst')
            else:
                print("Fatal - unknown protocol type {}".format(protocol_type))
        config[int(channel.getAttribute("id"))] = channel_filter_string
        #print("filter for {} - {}".format(channel.getAttribute("id"), channel_filter_string))
        return config

def print_usage_help():
    print('tcpdumpfilter_for_cme.py -x <MDP3ChannelConfigFile> -i <inputPath> -o <outputPath> -c <selectedChannels> -p <subprocesses>')
    print('for example - ')
    print('               python3 tcpdumpfilter_for_cme.py -x ./config.xml.20181122 -i ./20181119/cme-a_20181119-09\:00\:00.pcap -o ./output/test/ -c "310" -p 4')
    print('               python3 tcpdumpfilter_for_cme.py -x ./config.xml.20181122 -i /mnt/tank/var/reactor/p2d/captures/cme-a/20181119 -o ./output/test/ -c "310" -p 16')

def opt_args(argv):
    input_path = ''
    output_path = ''
    mdp3_config = ''
    subprocess_num = 8
    interested_channels = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:x:p:c:")
    except getopt.GetoptError:
        print_usage_help()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage_help()
            sys.exit()
        elif opt in ("-i", "--input"):
            input_path = arg
        elif opt in ("-o", "--output"):
            output_path = arg
        elif opt in ("-x", "--config"):
            mdp3_config = arg
        elif opt in ("-p", "--subprocess"):
            subprocess_num = int(arg)
        elif opt in ("-c", "--selectedChannels"):
            interested_channels = arg
    if not (mdp3_config.strip() and input_path.strip() and output_path.strip() and interested_channels.strip()):
        print_usage_help()
        sys.exit(2)
    return (mdp3_config, input_path, output_path, subprocess_num, interested_channels)

def getPcapFiles(path):
    ret = {}
    if os.path.isdir(path):
        file_list = os.listdir(path)
        for i in range(0, len(file_list)):
            if re.search(".+\.pcap$", file_list[i].strip()):
                file_path = os.path.join(path, file_list[i])
                if os.path.isfile(file_path):
                    ret[file_path] = os.path.getsize(file_path)
            else:
                print("skip {} as it is not a pcap".format(file_list[i]))
        return ret
    elif os.path.isfile(path):
        if re.search(".+\.pcap$", path.strip()):
            ret[path] = os.path.getsize(path)
        else:
            print("skip {} as it is not a pcap".format(path))
    return ret

def runSubProcess(files, index, filter):
    sub_start_time = time.time()
    (input, output) = files[index]
    command = "tcpdump -r {} -w {} {}".format(input, output, filter)
    #print(command)
    sub_process = subprocess.Popen(command, shell=True)
    print("run subprocess {} for {}".format(sub_process.pid, input))
    return (sub_process, sub_start_time)

if __name__ == '__main__':
    start_time = time.time()

    (mdp3_config, input_path, output_path, subprocess_num, interested_channels) = opt_args(sys.argv[1:])

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    #valid interested channels
    tmp = interested_channels.split(',')
    channels = []
    for c in tmp:
        if c.strip():
            channels.append(c.strip())
    if len(channels) == 0:
        print("no valid interested channels!")
        sys.exit(2)

    #parse config file and get tcpdump filter string
    channel_filters = parse_mdp3_config(mdp3_config)

    filter_string = "\'"
    for c in channels:
        channel_id = int(c)
        if channel_filters.__contains__(channel_id):
            filter_string += channel_filters[channel_id] + ' or'
        else:
            print("{} is not in config file {}".format(c, mdp3_config))
            sys.exit(2)

    filter_string = filter_string[:filter_string.rfind(' or')] + "\'"
    #print(filter_string)

    #get all input pcap files, and sorted by their size
    #allPcapFiles = sorted(getPcapFiles(input_path).items(), key=lambda kvp: kvp[1])
    allPcapFiles = getPcapFiles(input_path).items()

    if len(allPcapFiles) == 0:
        print("no pcap file!")
        sys.exit(0)

    #prepare output file name
    processFileInfo = []
    for (file, fsize) in allPcapFiles:
        output_file_name = "filtered_" + os.path.basename(file)
        processFileInfo.append((file, os.path.join(output_path, output_file_name)))

    subprocess_num = min(subprocess_num, len(processFileInfo))
    print("actually we will run {} subprocesses".format(subprocess_num))

    running_subprocess = {}
    waiting_file_index = 0
    for i in range(0, subprocess_num):
        running_subprocess[waiting_file_index]  = runSubProcess(processFileInfo, waiting_file_index, filter_string)
        waiting_file_index += 1

    if len(running_subprocess.items()) < len(processFileInfo):
        while waiting_file_index < len(processFileInfo):
            for index, (sub, sub_start_time) in running_subprocess.items():
                subresult = sub.poll()
                if subresult is not None:
                    #log error if subprocess is not terminated normally
                    if subresult != 0:
                        print("subprocess {} is terminated unexpectly. error {}".format(sub.pid, subresult))
                    print("subprocess {} used(less than) {}".format(sub.pid, time.time() - sub_start_time))

                    #still have pcap files waiting to be processed
                    if waiting_file_index < len(processFileInfo):
                        running_subprocess[index] = runSubProcess(processFileInfo, waiting_file_index, filter_string)
                        waiting_file_index += 1
                    else:
                        break
            time.sleep(0.1)

    for index, (sub, sub_start_time) in running_subprocess.items():
        subresult = sub.wait()
        if subresult != 0:
            print("subprocess {} is terminated unexpectly. error {}".format(sub.pid, subresult))
        print("subprocess {} used(less than) {}".format(sub.pid, time.time() - sub_start_time))

    end_time = time.time()
    print("all done! used {}".format(end_time - start_time))

