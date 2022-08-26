# Copyright (C) 2016 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from operator import attrgetter

import L4_simple_switch_13
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
#import csv


class L4SimpleMonitor13(L4_simple_switch_13.SimpleSwitch13):

    def __init__(self, *args, **kwargs):
        super(L4SimpleMonitor13, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)
        self.single_flows_growth = 0
        self.single_flows_curr = 1

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.debug('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.debug('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(10)
    
    def _mean_median(self,flows):
        mean_flows = 0
        median_flows = 0
        if len(flows)!=0:
            mean_flows = sum(flows)/len(flows)
            if len(flows)%2==0:
                median_flows = (sorted(flows)[len(flows)//2]+sorted(flows)[len(flows)//2-1])/2.0
            else:
                median_flows = sorted(flows)[len(flows)//2]

        return mean_flows,median_flows
    
    def _count_pair_flows(self,udp_flows,tcp_flows):
        count = 0
        u_flows = []
        t_flows = []
        u_flows = udp_flows.copy()
        t_flows = tcp_flows.copy()
        if len(u_flows)!=0:
            for f in u_flows:
                if [f[1],f[0],f[3],f[2]] in u_flows:
                    count = count +1
                    u_flows.remove(f)
                    u_flows.remove([f[1],f[0],f[3],f[2]])
        
        if len(t_flows)!=0:
            for f in t_flows:
                if [f[1],f[0],f[3],f[2]] in t_flows:
                    count = count +1
                    t_flows.remove(f)
                    t_flows.remove([f[1],f[0],f[3],f[2]])
        
        return count



    def _request_stats(self, datapath):
        self.logger.debug('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        flow_packet_count = []
        flow_byte_count = []
        flow_duration = []
        udp_flows = []
        tcp_flows = []
        pair_flows = 0
        single_flows = 0

        body = ev.msg.body
        #print(body)

        

        #print(body, type(body))

        # self.logger.info('datapath         '
        #                  'source ip           dst ip           '
        #                  'out-port       packets          bytes')
        # self.logger.info('---------------- '
        #                  '------------------ ----------------- '
        #                  '-------- ----------------   ----------------  ')
        for stat in sorted([flow for flow in body if flow.priority == 1],
                           key=lambda flow: (flow.match['ipv4_src'],
                                             flow.match['ipv4_dst'])):
            
            with open('/home/sdnonos/demo.txt', 'a', encoding='UTF8') as f:
                f.write(str(stat))
                f.write('\n')


            flow_packet_count.append(stat.packet_count)
            flow_byte_count.append(stat.byte_count)
            flow_duration.append(stat.duration_sec)
            if stat.match['ip_proto']==6:
                tcp_flows.append([stat.match['tcp_src'],stat.match['tcp_dst'],stat.match['ipv4_src'],stat.match['ipv4_dst']])
            elif stat.match['ip_proto']==17:
                udp_flows.append([stat.match['udp_src'],stat.match['udp_dst'],stat.match['ipv4_src'],stat.match['ipv4_dst']])
                
            

            # self.logger.info('%016x %17s %17s %8x %16d %16d',
            #                  ev.msg.datapath.id,
            #                  stat.match['ipv4_src'], stat.match['ipv4_dst'],
            #                  stat.instructions[0].actions[0].port,
            #                  stat.packet_count, stat.byte_count)
        
        total_flows = len(udp_flows)+len(tcp_flows)
        med_pkt,mean_pkt=self._mean_median(flow_packet_count)
        medbyte,meanbyte=self._mean_median(flow_byte_count)
        med_dur,mean_dur=self._mean_median(flow_duration)
        pair_flows = self._count_pair_flows(udp_flows,tcp_flows)
        single_flows = (len(udp_flows)+len(tcp_flows)-(pair_flows*2))

        #print('---------------------------------------x----------------------------')
        #print(self.single_flows_curr,single_flows,len(body))

        if single_flows > 0:
            self.single_flows_growth = ((single_flows - self.single_flows_curr)/self.single_flows_curr)*100
            self.single_flows_curr = single_flows
        

        if med_pkt > 0: 
            print('Las estadísticas son: \n Total de flujos: %d \n Tamaño medio y mediana de paquetes: %f - %f\n'
                ' Media de Bytes y mediana de Bytes: %f - %f\n Duración: %f - %f\n'
                ' Flujos únicos: %d \n Flujos pares: %d \n Crecimiento de flujos únicos: %f%%' 
                % (total_flows, med_pkt,mean_pkt,medbyte,meanbyte,med_dur,mean_dur,single_flows,pair_flows,self.single_flows_growth))




            

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body

        # self.logger.info('datapath         port     '
        #                  'rx-pkts  rx-bytes rx-error '
        #                  'tx-pkts  tx-bytes tx-error')
        # self.logger.info('---------------- -------- '
        #                  '-------- -------- -------- '
        #                  '-------- -------- --------')
        # for stat in sorted(body, key=attrgetter('port_no')):
        #     self.logger.info('%016x %8x %8d %8d %8d %8d %8d %8d',
        #                      ev.msg.datapath.id, stat.port_no,
        #                      stat.rx_packets, stat.rx_bytes, stat.rx_errors,
        #                      stat.tx_packets, stat.tx_bytes, stat.tx_errors)
