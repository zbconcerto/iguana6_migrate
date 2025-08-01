hl7.zsegment = {}
hl7.zsegment.parse = require 'hl7.zsegment'
dateparse = require 'date.parse'

local conn_FSCVSQLPRD01 = db.connect{api=db.SQL_SERVER,name='FSCVSQLPRD01_SQL', user='FIDELIS\svc_IguanaProd',password='*****',live=true} 
local conn_IRVPAYSQL101 = db.connect{api=db.SQL_SERVER,name='IRVPAYSQL101_HIECrawler', user='FIDELIS\svc_IguanaProd',password='*****',live=true}

function main(Data)

   
local Msg,name = hl7.parse{vmd="PreManage.vmd",data=Data}
local T = db.tables{vmd="PreManage.vmd",name='ADT'}

   
 if Msg:nodeName() == 'Catchall' then
      iguana.logError('Unexpected message type.'..Msg.MSH[9][1]..''..Msg.MSH[9][2])
         return true
      
     elseif (Msg.MSH:isNull() or Msg.EVN:isNull() or Msg.PID:isNull()) then  
    iguana.logError('Important'..Msg:nodeName()..'Missing important segments')
        return true
    
      else
       if name == 'ADT' then 

         local FileName = Msg.MSH[3][1]:nodeValue():upper().."_"..Msg.MSH[7]:nodeValue().."_"..Msg.MSH[10]:nodeValue():upper()..'.hl7'

         ProcessADT(T,Msg,FileName)
         
         local F = io.open('//fidelis.local/shares/Protected/Partner File Exchange/Import_Archive/PreManage/HL7/'..FileName, 'w')         
         F:write(Data)
         F:close()
         print('Wrote "'..FileName..'"')
         
         --Creating File for Development
         local D = io.open('//fidelis.local/Shares/Backups/HL7/DevelopmentFeed/'..FileName,'w')
         D:write(Data)
         D:close()
         
       end    
  return T
 
 end   
 
 iguana.stopOnError(false)
 iguana.logInfo('Error MessageID#'.. iguana.messageId())   
   --trace(FileName) 
   
end

function ProcessADT(T,Msg,FileName)
   
--MSH   
T.HL7PreManageMessage[1].SendingApplication=Msg.MSH[3][1]:nodeValue():upper()
T.HL7PreManageMessage[1].SendingFacility=Msg.MSH[4][1]:nodeValue():upper();
T.HL7PreManageMessage[1].ReceivingApplication=Msg.MSH[5][1]:nodeValue():upper()
T.HL7PreManageMessage[1].ReceivingFacility=Msg.MSH[6][1]:nodeValue():upper()
T.HL7PreManageMessage[1].DateTimeMessage=Msg.MSH[7]:T()   
T.HL7PreManageMessage[1].Security=Msg.MSH[8]:nodeValue()
T.HL7PreManageMessage[1].MessageType=Msg.MSH[9][1]:nodeValue()   
T.HL7PreManageMessage[1].MessageControlID=Msg.MSH[10]:nodeValue():upper()
T.HL7PreManageMessage[1].ProcessingID=Msg.MSH[11][1]:nodeValue()
T.HL7PreManageMessage[1].VersionID=Msg.MSH[12][1]:nodeValue()
--EVN
T.HL7PreManageMessage[1].EventCode=Msg.EVN[1]:nodeValue()
T.HL7PreManageMessage[1].EventDateTime=Msg.EVN[2]:T()
--PID
T.HL7PreManageMessage[1].PatientIDInternal=Msg.PID[3][1][1]:nodeValue():upper()
T.HL7PreManageMessage[1].PatientIDExternal=Msg.PID[2][1]:nodeValue():upper()   
T.HL7PreManageMessage[1].PatientFullName=Msg.PID[5][1][1][1]:nodeValue():upper()..', '..Msg.PID[5][1][2]:nodeValue():upper()
T.HL7PreManageMessage[1].PatientLastName=Msg.PID[5][1][1][1]:nodeValue():upper()
T.HL7PreManageMessage[1].PatientFirstName=Msg.PID[5][1][2]:nodeValue():upper()
T.HL7PreManageMessage[1].DateofBirth=Msg.PID[7]:T()
T.HL7PreManageMessage[1].Sex=Msg.PID[8]:nodeValue()
T.HL7PreManageMessage[1].PatientAddress=Msg.PID[11][1][1][1]..''..Msg.PID[11][1][2]..', '..Msg.PID[11][1][3]..', '..Msg.PID[11][1][4]..', '..Msg.PID[11][1][5]
T.HL7PreManageMessage[1].PhoneNumberHome=Msg.PID[13][1][1]:nodeValue()
--PV1   
T.HL7PreManageMessage[1].PatientType=Msg.PV1[2][2]:nodeValue()
T.HL7PreManageMessage[1].PatientClass=Msg.PV1[2][1][1]:nodeValue()
T.HL7PreManageMessage[1].AssignedPatientLocation=Msg.PV1[3][1]..', '..Msg.PV1[3][2]..' '..Msg.PV1[3][3]..' '..Msg.PV1[3][4][1]..' '..Msg.PV1[3][5]
T.HL7PreManageMessage[1].AttendingDoctor=Msg.PV1[7][1][3]:nodeValue():upper()
T.HL7PreManageMessage[1].HospitalService=Msg.PV1[10][1]..'-'..Msg.PV1[10][2]
T.HL7PreManageMessage[1].VisitNumber=Msg.PV1[19][1]:nodeValue()
T.HL7PreManageMessage[1].DischargeDisposition=Msg.PV1[36]:nodeValue()
T.HL7PreManageMessage[1].ServicingFacility=Msg.PV1[39]:nodeValue()
T.HL7PreManageMessage[1].AdmitDateTime=Msg.PV1[44]:T()
T.HL7PreManageMessage[1].DischargeDateTime=Msg.PV1[45]:T() 

--PV2
  if not Msg.PV2:isNull() then
        T.HL7PreManageMessage[1].AccommodationCode=Msg.PV2[2][1]:nodeValue()
        T.HL7PreManageMessage[1].AdmitReason=Msg.PV2[3][1]:nodeValue()
      end

-- DG1
if not Msg.DG1[1]:isNull() then
      for i=1,#Msg.DG1 do  
        MapDiagnosis(T.HL7PreManageDiagnosis[i],Msg.DG1[i],Msg) 
      end
   end  

 -- IN1
if not Msg.IN1[1]:isNull() then
      for i=1,#Msg.IN1 do  
        MapInsurance(T.HL7PreManageInsurance[i],Msg.IN1[i],Msg) 
      end
   end  
   
--if not Msg.ZCD:isNull() then
--      T.HL7PreManageMessage[1].PreManageURL=Msg.ZCD[2]:nodeValue()
--end      
   
T.HL7PreManageMessage[1].SourceSystem=Msg.MSH[3][1]:nodeValue():upper();
T.HL7PreManageMessage[1].SourceFileName= FileName;
T.HL7PreManageMessage[1].CreateBy= 'Iguana Interface Engine'
T.HL7PreManageMessage[1].CreateDate=os.date('%Y-%m-%d %H:%M:%S')  
   
conn_FSCVSQLPRD01:merge{data=T,live=true}
conn_IRVPAYSQL101:merge{data=T,live=true}

end   

function MapDiagnosis(DGx,DGy,DGz)
   DGx.MessageControlID=DGz.MSH[10]:nodeValue():upper()
   DGx.PatientIDInternal=DGz.PID[3][1][1]:nodeValue():upper()
   DGx.PatientIDExternal = DGz.PID[2][1]:nodeValue():upper()
   
   DGx.SetID = DGy[1]:nodeValue()
   DGx.DiagnosisCodingMethod=DGy[2]:nodeValue()
   DGx.DiagnosisCode=DGy[3][1]:nodeValue()
   DGx.DiagnosisDescription=DGy[3][2]:nodeValue()
   DGx.DiagnosisDateTime=DGy[5]:T()
   DGx.DiagnosisType=DGy[6]:nodeValue()  
   DGx.CreateBy = 'Iguana Interface Engine'
   DGx.CreateDate = os.date('%Y-%m-%d %H:%M:%S')   
return DGx   
end  

function MapInsurance(INx,INy,INz)
   INx.MessageControlID=INz.MSH[10]:nodeValue():upper()
   INx.PatientIDInternal=INz.PID[3][1][1]:nodeValue():upper()
   INx.PatientIDExternal = INz.PID[2][1]:nodeValue():upper()
   
   INx.SetID = INy[1]:nodeValue()
   INx.InsuranceCompanyName = INy[4][1][1]:nodeValue():upper()
   INx.PolicyNumber=INy[36]:nodeValue():upper()
   INx.CreateBy = 'Iguana Interface Engine'
   INx.CreateDate = os.date('%Y-%m-%d %H:%M:%S')   
return DGx   
end 