hl7.zsegment = {}
hl7.zsegment.parse = require 'hl7.zsegment'
dateparse = require 'date.parse'

local conn_IRVPAYSQL101 = db.connect{api=db.SQL_SERVER,name='IRVPAYSQL101_HIECrawler', user='CONCERTO\svc_IguanaProd',password='*****',live=true}

function main(Data)

   
local Msg,name = hl7.parse{vmd="Concerto_HL7_Inbound.vmd",data=Data}
local T = db.tables{vmd="Concerto_HL7_Inbound.vmd",name='ADT'}

-- This shows the non compact parsing mode
local ExpandedZED = hl7.zsegment.parse{data=Data, compact=false}
   
 if Msg:nodeName() == 'Catchall' then
      iguana.logError('Unexpected message type.'..Msg.MSH[9][1]..''..Msg.MSH[9][2])
         return true
      
     --elseif (Msg.MSH:isNull() or Msg.EVN:isNull() or Msg.PID:isNull()) then
    elseif (Msg.MSH:isNull() or Msg.PID:isNull()) then
    iguana.logError('Important'..Msg:nodeName()..'Missing important segments')
        return true
    
      else
       if name == 'ADT' then 
         
         local FileNameSendingApplication = Msg.MSH[3][1]:nodeValue():upper()
                  
         if Msg.MSH[6][1]:nodeValue():upper() == 'KHIE' or FileNameSendingApplication == 'EPIC' or FileNameSendingApplication == 'ADM' or FileNameSendingApplication == 'REG' then
             FileNameSendingApplication = 'KHIE'
         end
         
         local FileName = FileNameSendingApplication.."_"..Msg.MSH[7]:nodeValue().."_"..Msg.MSH[10]:nodeValue():upper()..'.hl7'
         local F
                  
         ProcessADT(T,Msg,ExpandedZED.ZPP,FileName,Data)
         
         --if Msg.MSH[3][1]:nodeValue():upper() == 'PATIENTPING_ADT' or Msg.MSH[3][1]:nodeValue():upper() == 'CMT' then
         --F = io.open('//fidelis.local/shares/Protected/Partner File Exchange/Import_Archive/HL7/'..Msg.MSH[3][1]:nodeValue():upper().."/"..FileName,'w')
         F = io.open('//ad.concertocare.com/shares/Protected/Partner File Exchange/Import_Archive/HL7/'..FileNameSendingApplication.."/"..FileName,'w')

         --Creating File for Development
         local D = io.open('//ad.concertocare.com/Shares/Backups/HL7/DevelopmentFeed/'..FileName,'w')
         D:write(Data)
         D:close()  
            
        -- else
           -- FileName = Msg.MSH[10]:nodeValue():upper()..'.hl7'
           -- F = io.open('//fidelis.local/shares/Protected/Partner File Exchange/Import_Archive/HL7/CliniSync/'..FileName,'w')
        -- end   
         F:write(Data)
         F:close()
         print('Wrote "'..FileName..'"')
         
         
                
              
       end    
  return T
 
 end   
 

   
end

function ProcessADT(T,Msg,ZPP1,FileName,Data)
zpparay = {} 
--MSH   
if Msg.MSH[6][1]:nodeValue():upper() == 'KHIE' or Msg.MSH[3][1]:nodeValue():upper() == 'EPIC' or Msg.MSH[3][1]:nodeValue():upper() == 'ADM' or Msg.MSH[3][1]:nodeValue():upper() == 'REG' then
   T.HL7Message[1].SendingApplication='KHIE'
else
   T.HL7Message[1].SendingApplication=Msg.MSH[3][1]:nodeValue():upper();
end
   
if Msg.MSH[3][1]:nodeValue():upper() == 'PATIENTPING_ADT' then
   T.HL7Message[1].SendingFacility=Msg.MSH[4][2]:nodeValue():upper()
else
   T.HL7Message[1].SendingFacility=Msg.MSH[4][1]:nodeValue():upper()
end 

T.HL7Message[1].RawHL7Message = Data   
T.HL7Message[1].SendingFacilityUniversalIDType=Msg.MSH[4][3]:nodeValue():upper() 
T.HL7Message[1].ReceivingApplication=Msg.MSH[5][1]:nodeValue():upper()
T.HL7Message[1].ReceivingFacility=Msg.MSH[6][1]:nodeValue():upper()..' '..Msg.MSH[6][2]:nodeValue():upper()
T.HL7Message[1].DateTimeMessage=Msg.MSH[7]:T()   
T.HL7Message[1].Security=Msg.MSH[8]:nodeValue()
T.HL7Message[1].MessageType=Msg.MSH[9][1]:nodeValue()   
T.HL7Message[1].MessageControlID=Msg.MSH[10]:nodeValue():upper()
T.HL7Message[1].ProcessingID=Msg.MSH[11][1]:nodeValue()
T.HL7Message[1].VersionID=Msg.MSH[12][1]:nodeValue()
--EVN
   if Msg.EVN[1]:nodeValue() == '' then
      T.HL7Message[1].EventCode=Msg.MSH[9][2]:nodeValue()     
   else  
      T.HL7Message[1].EventCode=Msg.EVN[1]:nodeValue()
   end   
   if Msg.EVN[2]:isNull() then
      T.HL7Message[1].EventDateTime=Msg.MSH[7]:T() 
   else
     T.HL7Message[1].EventDateTime=Msg.EVN[2]:T()
   end   
--PID
   if Msg.MSH[3][1]:nodeValue():upper()=='CLINISYNC' then
      x = 1
      while x <= 100 do
         if Msg.PID[3][x][4][1]:nodeValue()=='Concerto Healthcare' then
            T.HL7Message[1].PatientIDInternal=Msg.PID[3][x][1]:nodeValue():upper() 
             x = 101
         else   
             x = x + 1
         end   
      end                  
   elseif Msg.MSH[3][1]:nodeValue():upper()=='HEALTHIX_CEN' then
      T.HL7Message[1].PatientIDInternal=Msg.PID[4][1][1]:nodeValue()
   else
      T.HL7Message[1].PatientIDInternal=Msg.PID[3][1][1]:nodeValue():upper()   
   end
   
T.HL7Message[1].PatientIDExternal=Msg.PID[2][1]:nodeValue():upper()   
T.HL7Message[1].PatientFullName=Msg.PID[5][1][1][1]:nodeValue():upper()..', '..Msg.PID[5][1][2]:nodeValue():upper()..' '..Msg.PID[5][1][3]:nodeValue():upper()
T.HL7Message[1].PatientMiddleInitial=Msg.PID[5][1][3]:nodeValue():upper()   
T.HL7Message[1].PatientLastName=Msg.PID[5][1][1][1]:nodeValue():upper()
T.HL7Message[1].PatientFirstName=Msg.PID[5][1][2]:nodeValue():upper()
T.HL7Message[1].DateofBirth=Msg.PID[7]:T()
T.HL7Message[1].Sex=Msg.PID[8]:nodeValue()
T.HL7Message[1].PatientAlias=Msg.PID[9][1][1][1]:nodeValue()
T.HL7Message[1].Race=Msg.PID[10][1][1]..' '..Msg.PID[10][1][2]
T.HL7Message[1].PrimaryLanguage=Msg.PID[15][1]..'-'..Msg.PID[15][2]
T.HL7Message[1].MaritalStatus=Msg.PID[16][2]:nodeValue()
T.HL7Message[1].Religion=Msg.PID[17][1]..'-'..Msg.PID[17][2]
T.HL7Message[1].PatientAddress=Msg.PID[11][1][1][1]..', '..Msg.PID[11][1][2]..', '..Msg.PID[11][1][3]..', '..Msg.PID[11][1][4]..', '..Msg.PID[11][1][5]
T.HL7Message[1].PhoneNumberHome=Msg.PID[13][1][1]:nodeValue()      
T.HL7Message[1].PatientAccountnumber=Msg.PID[18][1]:nodeValue() 
T.HL7Message[1].AssigningFacility=Msg.PID[3][2][4][1]:nodeValue()  
--PV1   
T.HL7Message[1].PatientType=Msg.PV1[2][2]:nodeValue()
T.HL7Message[1].PatientClass=Msg.PV1[2][1][1]:nodeValue()
T.HL7Message[1].AssignedPatientLocation=Msg.PV1[3][1]..', '..Msg.PV1[3][2]..' '..Msg.PV1[3][3]..' '..Msg.PV1[3][4][1]..' '..Msg.PV1[3][5]
T.HL7Message[1].AttendingDoctor=Msg.PV1[7][1][1]..'-'..Msg.PV1[7][1][3]..' '..Msg.PV1[7][1][2][1]
T.HL7Message[1].ReferringDoctor=Msg.PV1[8][1][1]..'-'..Msg.PV1[8][1][3]..' '..Msg.PV1[8][1][2][1]
T.HL7Message[1].ConsultingDoctor=Msg.PV1[9][1][1]..'-'..Msg.PV1[9][1][3]..' '..Msg.PV1[9][1][2][1]   
T.HL7Message[1].HospitalService=Msg.PV1[10][1]..'-'..Msg.PV1[10][2]
T.HL7Message[1].AdmitSource=Msg.PV1[14][1]..' '..Msg.PV1[14][2]   
T.HL7Message[1].VisitNumber=Msg.PV1[19][1]:nodeValue()
T.HL7Message[1].DischargeDisposition=Msg.PV1[36]:nodeValue()
T.HL7Message[1].DischargedtoLocation=Msg.PV1[37][1][1]:nodeValue()
T.HL7Message[1].ServicingFacility=Msg.PV1[39][2]:nodeValue()
T.HL7Message[1].AdmitDateTime=Msg.PV1[44]:T()
T.HL7Message[1].DischargeDateTime=Msg.PV1[45]:T() 
   
local buildingValue = Msg.PV1[3][7]:nodeValue()
if buildingValue == "" then
   T.HL7Message[1].PatientBuilding = nil
else
   T.HL7Message[1].PatientBuilding = buildingValue
end

--PD1
  if not Msg.PV1:isNull() then
        T.HL7Message[1].PatientPrimaryCareProviderID=Msg.PD1[4][1][1]:nodeValue()
        T.HL7Message[1].PatientPrimaryCareProviderName=Msg.PD1[4][1][3]..' '..Msg.PD1[4][1][2][1]
      end
   
--PV2
  if not Msg.PV2:isNull() then
        T.HL7Message[1].AccommodationCode=Msg.PV2[2][1]:nodeValue()
        T.HL7Message[1].AdmitReason=Msg.PV2[3][1]:nodeValue()
      end
   
-- OBX
if not Msg.OBX[1]:isNull() then
      for i=1,#Msg.OBX do  
        MapObservationResult(T.HL7ObservationResult[i],Msg.OBX[i],Msg) 
      end
   end  
   
-- DG1cx
if not Msg.DG1[1]:isNull() then
      for i=1,#Msg.DG1 do  
        MapDiagnosis(T.HL7Diagnosis[i],Msg.DG1[i],Msg) 
      end
   end  

-- NK1
if not Msg.NK1[1]:isNull() then
      for i=1,#Msg.NK1 do  
        MapNK(T.HL7NextofKin[i],Msg.NK1[i],Msg) 
      end
   end  
   
-- IN1
if not Msg.IN1[1]:isNull() then
      for i=1,#Msg.IN1 do  
        MapInsurance(T.HL7Insurance[i],Msg.IN1[i],Msg) 
      end
   end 
   
--ZMD
T.HL7Message[1].ZMDText=Msg.ZMD[1]:nodeValue()   
   
-- ZPP   
if not Msg.ZPP:isNull() then    
 for i=1,#ZPP1 do               
         local typeof=type(ZPP1[i][2])
        
         if typeof=='string' then
            zpparay[i] =  tostring("\n"..ZPP1[i][2])
         else
	        zpparay[i] =  tostring("\n"..ZPP1[i][1][1][1][1].."-"..ZPP1[i][3])
         end
       i  = i+1
    end 
      T.HL7Message[1].ZPPText=table.concat(zpparay)
  end   
     
if Msg.MSH[6][1]:nodeValue():upper() == 'KHIE' or Msg.MSH[3][1]:nodeValue():upper() == 'EPIC' or Msg.MSH[3][1]:nodeValue():upper() == 'ADM' or Msg.MSH[3][1]:nodeValue():upper() == 'REG' then
   T.HL7Message[1].SourceSystem='KHIE'
else
   T.HL7Message[1].SourceSystem=Msg.MSH[3][1]:nodeValue():upper();
end
   
T.HL7Message[1].SourceFileName= FileName;  
T.HL7Message[1].CreateBy= 'Iguana Interface Engine'
T.HL7Message[1].CreateDate=os.date('%Y-%m-%d %H:%M:%S')  
   
conn_IRVPAYSQL101:merge{data=T,live=true}
end   

function MapNK(NKx,NKy,NKz)
   NKx.MessageControlID=NKz.MSH[10]:nodeValue():upper()
   NKx.PatientIDInternal=NKz.PID[3][1][1]:nodeValue():upper()
   NKx.PatientIDExternal = NKz.PID[2][1]:nodeValue():upper()
   
   NKx.SetID = NKy[1]:nodeValue()
   NKx.Name=NKy[2][1][2]..' '..NKy[2][1][1][1]
   NKx.Relationship=NKy[3][1]:nodeValue()
   NKx.Address=NKy[4][1][1][1]..', '..NKy[4][1][2]..', '..NKy[4][1][3]..', '..NKy[4][1][4]..', '..NKy[4][1][5]
   NKx.PhoneNumber=NKy[5][1][1]:nodeValue()
   NKx.MaritalStatus=NKy[14][2]:nodeValue()
   NKx.Sex=NKy[15]:nodeValue()
   NKx.DateofBirth=NKy[16]:T()
   NKx.CreateBy = 'Iguana Interface Engine'
   NKx.CreateDate = os.date('%Y-%m-%d %H:%M:%S')   
return NKx   
end  

function MapObservationResult(OBx,OBy,OBz)
   OBx.MessageControlID=OBz.MSH[10]:nodeValue():upper()
   
   OBx.SetID = OBy[1]:nodeValue()
   OBx.ValueType = OBy[2]:nodeValue()
   OBx.ObservationID = OBy[3][1]:nodeValue()
   OBx.ObservationText = OBy[3][2]:nodeValue()
   OBx.ObservationName = OBy[3][3]:nodeValue()
   OBx.ObservationAltID = OBy[3][4]:nodeValue()
   OBx.ObservationAltText = OBy[3][5]:nodeValue()
   OBx.ObservationValue = OBy[5][1][1]:nodeValue()

   OBx.Units = OBy[6][1]:nodeValue()
   OBx.ReferencesRange = OBy[7]:nodeValue()
   OBx.DateReference = OBy[14]:T()
   OBx.UserDefinedAccessCheck = OBy[15][1]:nodeValue()  
   
   OBx.CreateBy = 'Iguana Interface Engine'
   OBx.CreateDate = os.date('%Y-%m-%d %H:%M:%S')   
return OBx   
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
   INx.InsurancePlanID= INy[2][1]
   INx.InsuranceCompanyID=INy[3][1][1]
   INx.InsuranceCompanyName = INy[4][1][1]:nodeValue():upper()
   INx.InsuranceCompanyAddress=INy[5][1][1][1]..', '..INy[5][1][3]..', '..INy[5][1][4]..', '..INy[5][1][5]
   INx.InsuranceCoPhoneNumber=INy[7][1][1]:nodeValue()
   INx.PlanEffectiveDate=INy[12]:T()  
   INx.PolicyNumber=INy[36]:nodeValue():upper()
   INx.CreateBy = 'Iguana Interface Engine'
   INx.CreateDate = os.date('%Y-%m-%d %H:%M:%S')   
   
--IN2
  if not INz.IN2:isNull() then
         INx.InsuredSSN=INz.IN2[2]:nodeValue() 
      end   
return INx   
end    