#define _WIN32_DCOM				// Enables DCOM extensions
#define INITGUID				// Initialize OLE constants
#define ECL_SID "opc.askuer"	// identificator of OPC server
#define OPC_PORT	4988		// Порт сервера
//#define LOGS	1

#include <stdio.h>
#include <math.h>				// some mathematical function
#include "server.h"				// server variable
#include "unilog.h"				// universal utilites for creating log-files
#include <locale.h>				// set russian codepage
#include <opcda.h>				// basic function for OPC:DA
#include "lightopc.h"			// light OPC library header file
#include "serialport.h"			// serial port header
#include "nport.h"				// Nport function

#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
#include <mbstring.h>
#include "dbaccess.h"

//#include "Stackwalker.h"				// Nport function
#include <winsock.h>
//---------------------------------------------------------------------------------
static const loVendorInfo vendor = {0,1,11,0,"ASKUER OPC Server" };	// OPC vendor info (Major/Minor/Build/Reserv)
static void a_server_finished(VOID*, loService*, loClient*);		// OnServer finish his work
static int OPCstatus=OPC_STATUS_RUNNING;							// status of OPC server
loService *my_service;			// name of light OPC Service
dbaccess dbase;
//---------------------------------------------------------------------------------
SerialPort port[PORT_NUM_MAX];													// com-port
UINT com_num[PORT_NUM_MAX]={2,3,4,5,6,7,8,9,10,11};								// COM-port numbers
UINT speed[PORT_NUM_MAX]={1200,1200,1200,1200,1200,1200,1200,1200,1200,1200};	// COM-port speed
UINT parity=2;					// Parity
UINT databits=8;				// Data bits 
UINT preconfig=0;
WCHAR bufMB[2501];
//SHORT SuccessRead[MAX_LOGIKS_NUM][30][24];
//---------------------------------------------------------------------------------
HANDLE  hReadEndEvents [PORT_NUM_MAX+1];	// Events signal about thread ended
BOOL	ReadEndFlag[PORT_NUM_MAX+1];		// CurrentProgressThread
DWORD	dwThrdID;							// Newlycreated thread ID value
HANDLE	hThrd[PORT_NUM_MAX+1];				// Newlycreated thread handle
//VOID	PollDeviceEtherNet (LPVOID lpParam);// polling group device thread
VOID	PollDeviceCOM (LPVOID lpParam);		// polling group device thread
//---------------------------------------------------------------------------------
UINT refresh[14]={1,1,1,1,8,3,10,15,7,20,5,4,3,2};	// refresh data
UINT timers[14]= {1,2,3,3,5,2,1,2,2,2,2,2,2,2};		// timers
UINT Com_number=0, IP_number=0; // порты и ипшники, которые найдены в конфиге
UINT LogikNum=0, WinconNum=0;	// device numbers
UINT devNum=0;					// main device nums
UINT tag_num=0;					// tags counter
VOID lScanBus (LPVOID lpParam);	// scan logika (SP Bus)
INT  tScanBus (SHORT blok);		// scan logika 941, 943 (RS Bus)
//-----------------------------------------------------------------------------
UCHAR sBuf3[] = {0x10,0x01,0x10,0x1F,0x1D,0x10,0x2,0x9,0x30,0x9,0x30,0x30,0x33,0x0C,0x10,0x03,0x0,0x0,0x0};	// 003
UCHAR sBuf99[] = {0x10,0x01,0x10,0x1F,0x1D,0x10,0x2,0x9,0x30,0x9,0x30,0x39,0x39,0x0C,0x10,0x03,0x0,0x0,0x0};	// 099
UCHAR sBuf52[] = {0x10,0x01,0x10,0x1F, 0xC,0x10,0x2,0x9,0x30,0x9,0x30,0x35,0x32,0x9,0x30,0x9,0x30,0x30,0x31,0xC,0x10,0x03,0x0,0x0,0x0};	// 052(0)
//---------------------------------------------------------------------------------
unilog *logg=NULL;				// new structure of unilog
AnsLog alog[PORT_NUM_MAX];		// structure of answer from logika
DplExch	dpl[MAX_WINCON_PARAM];	// data from one wincon
FILE *CfgFile;					// pointer to .ini file
DataR DR[TAGS_NUM_MAX];			// data
//---------------------------------------------------------------------------------
BOOL eConnection (INT _s_port, SOCKET &sck, CHAR* strC, UINT sock);
VOID ReadMConf (INT dev, SHORT blok, CHAR* name, UINT tagn);
CHAR* FormSequence (UCHAR lNum, UCHAR nFunc, UCHAR nChannal, UINT npAdr, UCHAR nPipe, SPrint Msp);
UINT PollDevice(UINT device);	// polling single device
UINT ScanBus();					// bus scanned programm
INT	 init_tags(VOID);			// Init tags
UINT InitDriver();				// func of initialising port and creating service
UINT DestroyDriver();			// function of detroying driver and service
BOOL AnalyseSequence (CHAR* dat, UINT len, UINT blok);
BOOL AnalyseAnswer (CHAR* dat,UINT len, UINT blok, SHORT pAdr);
BOOL ReadAPC79 (SHORT adr, SHORT blok);
BOOL ReadCPT (SHORT adr, SHORT blok, SHORT type, SHORT pAdr);
SHORT KScode (BYTE *msg, INT len);
SHORT CRCode (BYTE *msg, INT len);
VOID CheckRegStatus ();
VOID ReadLData (SHORT blok);
DOUBLE cIEEE754toFloat (UCHAR *Data);	// convert IEEE754 format to Float value
VOID WriteToPort (UINT com, UINT device, CHAR* Out);
//---------------------------------------------------------------------------------
static CRITICAL_SECTION lk_values;	// protects ti[] from simultaneous access 
static INT mymain(HINSTANCE hInstance, INT argc, CHAR *argv[]);
static INT show_error(LPCTSTR msg);		// just show messagebox with error
static INT show_msg(LPCTSTR msg);		// just show messagebox with message
static VOID poll_device(VOID);			// function polling device
CHAR* ReadParam (CHAR *SectionName,CHAR *Value);	// read parametr from .ini file
CRITICAL_SECTION drv_access;
BOOL WorkEnable=TRUE;
//---------------------------------------------------------------------------------
//CHAR WincName[TAGS_NUM_MAX][10];
CHAR argv0[FILENAME_MAX + 32];	// lenght of command line (file+path (260+32))
static CHAR *tn[TAGS_NUM_MAX];		// Tag name
static loTagValue tv[TAGS_NUM_MAX];	// Tag value
static loTagId ti[TAGS_NUM_MAX];	// Tag id
UINT	tTotal=0;				// total quantity of tags
//HRESULT WriteDevice(INT device,const unsigned cmdnum,LPSTR data);	// write tag to device
//---------------------------------------------------------------------------------
// {EA8767A6-B541-4dc2-8361-6F2273B054B7}
DEFINE_GUID(GID_askuerOPCserverDll, 
0xea8767a6, 0xb541, 0x4dc2, 0x83, 0x61, 0x6f, 0x22, 0x73, 0xb0, 0x54, 0xb7);
// {87988D6B-0D46-4f03-9178-23505D3DE111}
DEFINE_GUID(GID_askuerOPCserverExe, 
0x87988d6b, 0xd46, 0x4f03, 0x91, 0x78, 0x23, 0x50, 0x5d, 0x3d, 0xe1, 0x11);
//---------------------------------------------------------------------------------
VOID timetToFileTime( time_t t, LPFILETIME pft )
{   LONGLONG ll = Int32x32To64(t, 10000000) + 116444736000000000;
    pft->dwLowDateTime = (DWORD) ll;
    pft->dwHighDateTime =  (ULONG)(ll >>32);}
CHAR *absPath(CHAR *fileName)					// return abs path of file
{ static char path[sizeof(argv0)]="\0";			// path - massive of comline
  CHAR *cp;
  if(*path=='\0') strcpy(path, argv0);
  if(NULL==(cp=strrchr(path,'\\'))) cp=path; else cp++;
  cp=strcpy(cp,fileName);
  return path;}

inline void init_common(void)		// create log-file
{ logg = unilog_Create(ECL_SID, absPath(LOG_FNAME), NULL, 0, ll_DEBUG); // level [ll_FATAL...ll_DEBUG] 
  UL_INFO((LOGID, "Start"));}
inline void cleanup_common(void)	// delete log-file
{ 
  UL_INFO((LOGID, "Finish"));  
  unilog_Delete(logg); logg = NULL;
  UL_INFO((LOGID, "total Finish"));
 // DeInitAllocCheck();
}

INT show_error(LPCTSTR msg)			// just show messagebox with error
{ ::MessageBox(NULL, msg, ECL_SID, MB_ICONSTOP|MB_OK);
  return 1;}
INT show_msg(LPCTSTR msg)			// just show messagebox with message
{ ::MessageBox(NULL, msg, ECL_SID, MB_OK);
  return 1;}

inline void cleanup_all(DWORD objid)
{ // Informs OLE that a class object, previously registered is no longer available for use  
  if (FAILED(CoRevokeClassObject(objid)))  UL_WARNING((LOGID, "CoRevokeClassObject() failed..."));
  DestroyDriver();					// close port and destroy driver
  CoUninitialize();					// Closes the COM library on the current thread
  cleanup_common();					// delete log-file
  //fclose(CfgFile);					// close .ini file
}
//---------------------------------------------------------------------------------
#include "opc_main.h"	//	main server 
//---------------------------------------------------------------------------------
INT APIENTRY WinMain(HINSTANCE hInstance,HINSTANCE hPrevInstance,LPSTR lpCmdLine,INT nCmdShow)
{ static char *argv[3] = { "dummy.exe", NULL, NULL };	// defaults arguments
  argv[1] = lpCmdLine;									// comandline - progs keys
  return mymain(hInstance, 2, argv);}

INT main(INT argc, CHAR *argv[])
{  return mymain(GetModuleHandle(NULL), argc, argv); }
//---------------------------------------------------------------------------------
INT mymain(HINSTANCE hInstance, INT argc, CHAR *argv[]) 
{
//InitAllocCheck();{
const char eClsidName [] = ECL_SID;				// desription 
const char eProgID [] = ECL_SID;				// name
DWORD objid;									// fully qualified path for the specified module
CHAR *cp;
objid=::GetModuleFileName(NULL, argv0, sizeof(argv0));	// function retrieves the fully qualified path for the specified module
if(objid==0 || objid+50 > sizeof(argv0)) return 0;		// not in border

init_common();									// create log-file
if(NULL==(cp = setlocale(LC_ALL, ".1251")))		// sets all categories, returning only the string cp-1251
	{ 
	UL_ERROR((LOGID, "setlocale() - Can't set 1251 code page"));	// in bad case write error in log
	cleanup_common();							// delete log-file
    return 0;
	}
cp = argv[1];		
if(cp)						// check keys of command line 
	{
    INT finish = 1;			// flag of comlection
    if (strstr(cp, "/r"))	//	attempt registred server
		{
	     if (loServerRegister(&GID_askuerOPCserverExe, eProgID, eClsidName, argv0, 0)) 
			{ show_error("Registration Failed");
			  UL_ERROR((LOGID, "Registration <%s> <%s> Failed", eProgID, argv0));  } 
		 else 
			{ show_msg("ASKUER OPC Registration Ok");
			 UL_INFO((LOGID, "Registration <%s> <%s> Ok", eProgID, argv0));		}
		} 
	else 
		if (strstr(cp, "/u")) 
			{
			 if (loServerUnregister(&GID_askuerOPCserverExe, eClsidName)) 
				{ show_error("UnRegistration Failed");
				 UL_ERROR((LOGID, "UnReg <%s> <%s> Failed", eClsidName, argv0)); } 
			 else 
				{ show_msg("ASKUER OPC Server Unregistered");
				 UL_INFO((LOGID, "UnReg <%s> <%s> Ok", eClsidName, argv0));		}
			} 
		else  // only /r and /u options
			if (strstr(cp, "/?")) 
				 show_msg("Use: \nKey /r to register server.\nKey /u to unregister server.\nKey /? to show this help.");
				 else
					{
					 UL_WARNING((LOGID, "Ignore unknown option <%s>", cp));
					 finish = 0;		// nehren delat
					}
		if (finish) {      cleanup_common();      return 0;    } 
	}
if ((CfgFile = fopen(CFG_FILE, "r+")) == NULL)
	{	
	 show_error("Error open .ini file");
	 UL_ERROR((LOGID, "Error open .ini file"));	// in bad case write error in log
	 return 0;
	}
if (FAILED(CoInitializeEx(NULL, COINIT_MULTITHREADED))) 
	{	// Initializes the COM library for use by the calling thread
     UL_ERROR((LOGID, "CoInitializeEx() failed. Exiting..."));
     cleanup_common();	// close log-file
     return 0;
	}
UL_INFO((LOGID, "CoInitializeEx() Ok...."));	// write to log

if (InitDriver()) {		// open and set com-port
    CoUninitialize();	// Closes the COM library on the current thread
    cleanup_common();	// close log-file
    return 0;
  }
UL_INFO((LOGID, "InitDriver() Ok...."));	// write to log

if (FAILED(CoRegisterClassObject(GID_askuerOPCserverExe, &my_CF, 
				   CLSCTX_LOCAL_SERVER|CLSCTX_REMOTE_SERVER|CLSCTX_INPROC_SERVER, 
				   REGCLS_MULTIPLEUSE, &objid)))
    { UL_ERROR((LOGID, "CoRegisterClassObject() failed. Exiting..."));
      cleanup_all(objid);		// close comport and unload all librares
      return 0; }
UL_INFO((LOGID, "CoRegisterClassObject() Ok...."));	// write to log

Sleep(1000);
my_CF.Release();		// avoid locking by CoRegisterClassObject() 

if (OPCstatus!=OPC_STATUS_RUNNING)	// ???? maybe Status changed and OPC not currently running??
	{	while(my_CF.in_use())      Sleep(1000);	// wait
		cleanup_all(objid);
		return 0;	}
while(my_CF.in_use())	// while server created or client connected
	{
 	 CheckRegStatus ();
	 if (WorkEnable)
	     poll_device();		// polling devices else do nothing (and be nothing)	 
	 for (UINT w=0;w<14;w++) 
		{
		 if (timers[w]) timers[w]--; else timers[w]=refresh[w];	 
		 if (refresh[w]==1) timers[w]=0;
		}
	}
UL_INFO((LOGID, "end cleanup_all()"));	// write to log
cleanup_all(objid);		// destroy himself
//}
return 0;
}
//-------------------------------------------------------------------
loTrid ReadTags(const loCaller *, unsigned  count, loTagPair taglist[],
		VARIANT   values[],	WORD      qualities[],	FILETIME  stamps[],
		HRESULT   errs[],	HRESULT  *master_err,	HRESULT  *master_qual,
		const VARTYPE vtype[],	LCID lcid)
{  return loDR_STORED; }
//-------------------------------------------------------------------
INT WriteTags(const loCaller *ca,
              unsigned count, loTagPair taglist[],
              VARIANT values[], HRESULT error[], HRESULT *master, LCID lcid)
{  return loDW_TOCACHE; }
//-------------------------------------------------------------------
VOID activation_monitor(const loCaller *ca, INT count, loTagPair *til){}
//-------------------------------------------------------------------
UINT InitDriver()
{
 UL_INFO((LOGID, "InitDriver()"));
 loDriver ld;		// structure of driver description
 LONG ecode;		// error code 
 tTotal = TAGS_NUM_MAX;		// total tag quantity
 if (my_service) {	
      UL_ERROR((LOGID, "Driver already initialized!"));
      return 0;
  }
 memset(&ld, 0, sizeof(ld));   
 ld.ldRefreshRate =5000;		// polling time 
 ld.ldRefreshRate_min = 4000;	// minimum polling time
 ld.ldWriteTags = WriteTags;	// pointer to function write tag
 ld.ldReadTags = ReadTags;		// pointer to function read tag
 ld.ldSubscribe = activation_monitor;	// callback of tag activity
 ld.ldFlags = loDF_IGNCASE;				// ignore case
 ld.ldBranchSep = '/';					// hierarchial branch separator
 ecode = loServiceCreate(&my_service, &ld, tTotal);		//	creating loService 
 UL_TRACE((LOGID, "%!e loServiceCreate()=", ecode));	// write to log returning code
 if (ecode) return 1;									// error to create service	
 InitializeCriticalSection(&lk_values);
 // COM ports -------------------------------------------------------------------------------------------------
 COMMTIMEOUTS timeouts;
 timeouts.ReadIntervalTimeout = 3;
 timeouts.ReadTotalTimeoutMultiplier = 0;	//0
 timeouts.ReadTotalTimeoutConstant = 80;				// !!! (180)
 timeouts.WriteTotalTimeoutMultiplier = 00;		//0
 timeouts.WriteTotalTimeoutConstant = 25;	//25
 
 preconfig = atoi(ReadParam ("Config","Preconfig"));

 for (UINT i=1,j=0;i<PORT_NUM_MAX;i++)
	{
	 sprintf (argv0,"Port%d",i);
	 com_num[j] = atoi(ReadParam (argv0,"COM"));
	 speed[j]	= atoi(ReadParam (argv0,"Speed"));
	 if (com_num[j] && speed[j])
		{
		 INT res=sio_checkalive (com_num[j], 3000);
		 switch (res)
			{
			 case D_ALIVE_CLOSE: UL_INFO((LOGID, "This port is alive and is not used by any program")); break;
			 case D_ALIVE_OPEN: UL_INFO((LOGID, "This port is alive and is used by some program")); break;
			 case D_NOT_ALIVE: UL_INFO((LOGID, "This port is not alive")); break;
			 case D_NOT_NPORT_COM: UL_INFO((LOGID, "The COM port number is not belonged to any configured NPort Server on the host")); break;
			 case D_SOCK_INIT_FAIL:	UL_INFO((LOGID, "Initialize the WinSock fail")); break;
			}
		 UL_INFO((LOGID, "Opening port COM%d on speed %d with parity %d and databits %d",com_num[j],speed[j], parity, databits));
		 if (parity==0) if (!port[j].Open(com_num[j],speed[j], SerialPort::EvenParity, databits, SerialPort::OneStopBit, SerialPort::NoFlowControl, FALSE)) { UL_ERROR((LOGID, "Error open COM-port")); continue;}
		 if (parity==2) if (!port[j].Open(com_num[j],speed[j], SerialPort::NoParity, databits, SerialPort::OneStopBit, SerialPort::NoFlowControl, FALSE)) { UL_ERROR((LOGID, "Error open COM-port")); continue;}
		 port[j].SetTimeouts(timeouts);
		 UL_INFO((LOGID, "Set COM-port timeouts %d:%d:%d:%d:%d",timeouts.ReadIntervalTimeout,timeouts.ReadTotalTimeoutMultiplier,timeouts.ReadTotalTimeoutConstant,timeouts.WriteTotalTimeoutMultiplier,timeouts.WriteTotalTimeoutConstant));
		 j++;
		}
	} Com_number=j;	
 dbase.sqlconn((UCHAR FAR *)"askuer",(UCHAR FAR *)"root",(UCHAR FAR *)"");

 // EtherNet -------------------------------------------------------------------------------------------------
 UL_INFO((LOGID, "Scan bus [C=%d E=%d]",Com_number,IP_number));
 if (ScanBus()) 
	{ 
	 UL_INFO((LOGID, "Total %d devices found",devNum)); 
	 if (init_tags()) return 1; 
	 else			return 0;
	}
 else		
	{ UL_ERROR((LOGID, "No devices found")); 
 	  return 1; } 
}
//----------------------------------------------------------------------------------------
UINT ScanBus()
{
 LogikNum=0; WinconNum=0; devNum=0;
// INT found=0;
 INT ecode=0;
 UL_INFO((LOGID, "ScanBus started"));
 DWORD start=GetTickCount();
 // COM ports -------------------------------------------------------------------------------------------------
 for (UINT i=0; i<Com_number; i++)
	{	 
 	 hThrd[i] = CreateThread (NULL, 0, (LPTHREAD_START_ROUTINE) lScanBus, (LPVOID)i, 0, &dwThrdID);
	 ReadEndFlag[i]=false;	 
	 Sleep(5000);
	}
 ecode=Com_number;
 while (1)
	{
	 for (UINT i=0; i<Com_number; i++)
		{
		 if (ReadEndFlag[i]) 
			{
			 ecode--;
			 ReadEndFlag[i]=false;
			}
		 Sleep (1000);
		}
	 if (!ecode) 
		{
		 //LogikNum=devNum;
		 UL_DEBUG((LOGID, "All threads are complete. Total %d seconds.",(GetTickCount()-start)/1000));
		 UL_DEBUG((LOGID, "Found %d logiks",LogikNum));
		 break;
		}
	}
 // EtherNet -------------------------------------------------------------------------------------------------
 tTotal = LogikNum+WinconNum;
 UL_INFO((LOGID, "Total %d devices",tTotal));	
 return tTotal;
}
//-----------------------------------------------------------------------------------
INT tScanBus (SHORT blok)
{
 UINT devBus=0; CHAR name[20]; CHAR tmp[20];
 UL_TRACE((LOGID, "tScanBus [%d/%d]", blok, Com_number));
 for (INT adr=1;adr<=/*DEVICE_NUM_MAX*/11;adr++)
 for (INT nump=0;nump<=0;nump++)
	{
	 UL_TRACE((LOGID, "ReadCPT [%d/%d]", adr, blok));
	 if (preconfig || ReadCPT (adr, blok, 0, 0))
	 if (preconfig || alog[blok].checksym)
		{
		 if ((UCHAR)alog[blok].data[0][0]==0x92 && (UCHAR)alog[blok].data[0][1]==0x29) sprintf (DeviceRU[devNum].name,"СПТ941.%d",alog[blok].data[2]);
		 else strcpy (DeviceRU[devNum].name,alog[blok].data[0]);
		 sprintf (DeviceRU[devNum].name,"СПТ941.5518508");
		 UL_DEBUG((LOGID, "[%s]", DeviceRU[devNum].name));
		 DeviceRU[devNum].tag_num = 0;  
		 if (preconfig || ReadCPT (adr, blok, 0, 36))
		 if (preconfig || alog[blok].checksym)
			{
			 strcpy (name,"           ");
			 strcpy (name,alog[blok].data[0]);
			 if (preconfig && speed[blok]==2400)
				{
				 sprintf (argv0,"com%d",com_num[blok]);
				 if (adr<10) sprintf (tmp,"0%d",adr);
				 else sprintf (tmp,"%d",adr);
				 strcpy (name,ReadParam (argv0,tmp));
				}
			 if (strlen(name))
				{
				 name[9]=name[7]; name[8]=name[6]; name[7]=name[5]; name[6]=name[4]; name[5]='-'; name[4]=name[3]; name[3]=name[2]; name[2]='-'; name[10]=0;
				 if (name[6]=='1' && name[7]=='1') name[6]=name[7]='2';
				 UL_DEBUG((LOGID, "[%s]", name));
				 for (UINT i=6;i<=6+MAX_PIPE_NUMS;i++)
					{
					 BOOL ne=FALSE;
					 strcpy (DeviceRU[devNum].name,name);
					 switch (name[i])
						{
						 case 48: UL_INFO((LOGID,"Опана пусто")); break;
						 case 49: UL_INFO((LOGID,"Не будем мерить тепло-воду.")); break;
						 case 50: if (DeviceRU[devNum].name[i+1]=='2')
									{	// тепло и теплофикационная вода
									 DeviceRU[devNum].type=22;
									 DeviceRU[devNum].com = blok; DeviceRU[devNum].device = adr;
									 DeviceRU[devNum].speed = speed[blok]; DeviceRU[devNum].status = 1;
									 DeviceRU[devNum].pipe=i-5;
									 DeviceRU[devNum].tag_num = sizeof(Archive941_2)/sizeof(Archive961_2[0]);
									 DeviceRU[devNum].name[6]='2'; DeviceRU[devNum].name[7]=0;
									 UL_INFO((LOGID,"Device found [%s] on address %d. type=%d. tags=%d pipe=%d", DeviceRU[devNum].name, adr,DeviceRU[devNum].type,DeviceRU[devNum].tag_num,DeviceRU[devNum].pipe)); 
									 ReadMConf (devNum, adr, DeviceRU[devNum].name, DeviceRU[devNum].tag_num);
									 devNum++; devBus++; i++;
									 strcpy (DeviceRU[devNum].name,DeviceRU[devNum-1].name);
									 DeviceRU[devNum].com = blok; DeviceRU[devNum].device = adr;
									 DeviceRU[devNum].speed = speed[blok]; DeviceRU[devNum].status = 1;
									 DeviceRU[devNum].type=21;
									 DeviceRU[devNum].pipe=i-6;
									 DeviceRU[devNum].tag_num = sizeof(Archive941_1)/sizeof(Archive941_1[0]);
									 DeviceRU[devNum].name[6]='1'; DeviceRU[devNum].name[7]=0;
									 UL_INFO((LOGID,"Device found [%s] on address %d. type=%d. tags=%d pipe=%d", DeviceRU[devNum].name, adr,DeviceRU[devNum].type,DeviceRU[devNum].tag_num,DeviceRU[devNum].pipe)); 
									 ReadMConf (devNum, adr, DeviceRU[devNum].name, DeviceRU[devNum].tag_num);
									 devNum++; devBus++;
									}
								 else	
									{	// просто тепло
									 DeviceRU[devNum].type=22;
									 DeviceRU[devNum].pipe=i-5;
									 DeviceRU[devNum].tag_num = sizeof(Archive941_2)/sizeof(Archive941_2[0]);
									 DeviceRU[devNum].name[6]='2'; DeviceRU[devNum].name[7]=0;
									 ne=TRUE;
									}
								 break;
						 case 52: DeviceRU[devNum].type =24; DeviceRU[devNum].pipe=i-5;
								  DeviceRU[devNum].tag_num = sizeof(Archive941_4)/sizeof(Archive941_4[0]);
								  DeviceRU[devNum].name[6]='3'; DeviceRU[devNum].name[7]=0; ne=TRUE; break; 
						 default: break;
						}
					 if (ne)
						{
						 DeviceRU[devNum].com = blok; DeviceRU[devNum].device = adr;
						 DeviceRU[devNum].speed = speed[blok]; DeviceRU[devNum].status = 1;
						 UL_INFO((LOGID,"[%d] Device found [%s] on address %d. type=%d. tags=%d pipe=%d",blok, DeviceRU[devNum].name, adr,DeviceRU[devNum].type,DeviceRU[devNum].tag_num,DeviceRU[devNum].pipe)); 
						 ReadMConf (devNum, adr, DeviceRU[devNum].name, DeviceRU[devNum].tag_num);
						 devNum++; devBus++; ne=FALSE;
						}
					}
				}//tr
			}//ch
		  else UL_INFO((LOGID,"wrong checksym"));
		}
	 else UL_INFO((LOGID,"wrong checksym"));
	}
 UL_INFO((LOGID,"Total %d logiks found on port Com%d",devBus,com_num[blok]));
 return devBus;
}
//-----------------------------------------------------------------------------------
VOID lScanBus (LPVOID lpParam)
{
 SHORT blok = (UINT) lpParam;
 UINT devBus=0; CHAR name[20]; CHAR tmp[20];
 UL_TRACE((LOGID, "lScanBus [%d/%d]", blok, Com_number));
 DWORD ctime=GetTickCount(); DWORD etime=GetTickCount();
  
 if ((ReadAPC79 (3,blok) && ReadAPC79 (99,blok) && ReadAPC79 (52,blok)) || preconfig)
	{	 
	 UL_TRACE((LOGID, "ReadAPC [%d/%d] time=%f", blok, Com_number,(DOUBLE)GetTickCount()-ctime));
	 DeviceRU[devNum].com = blok;
	 DeviceRU[devNum].device = 0;	 
	 strcpy (DeviceRU[devNum].name,apc[blok].version);
	 DeviceRU[devNum].speed=speed[blok];
	 DeviceRU[devNum].status = 1;
	 DeviceRU[devNum].tag_num=0;
	 DeviceRU[devNum].type=2;
	 UL_INFO((LOGID,"Device found [%s] on address %d",DeviceRU[devNum].name, 0));	// write in log
	 //devNum++; devBus++; 
	 
	 for (INT adr=1;adr<=DEVICE_NUM_MAX;adr++)
		{
		 ctime=GetTickCount();
	     UL_TRACE((LOGID, "ReadCPT()"));
		 if (preconfig || ReadCPT (adr, blok, 1, 8))
		 //if (preconfig)
		 if ((alog[blok].checksym && alog[blok].from==adr) || preconfig)
			{
			 UL_TRACE((LOGID, "ReadCPT(8) time=%f", (DOUBLE)GetTickCount()-ctime));
			 strcpy (DeviceRU[devNum].name,alog[blok].data[0]);
			 if (preconfig && speed[blok]==1200)
				{
				 sprintf (argv0,"com%d",com_num[blok]);
				 if (adr<10) sprintf (tmp,"0%d",adr);
				 else sprintf (tmp,"%d",adr);
				 strcpy (DeviceRU[devNum].name,ReadParam (argv0,tmp));
				 if (strlen(DeviceRU[devNum].name)) alog[blok].checksym=TRUE;
				 else alog[blok].checksym=FALSE;
				}
			 DeviceRU[devNum].tag_num = 0;
			 ctime=GetTickCount();
			 //if (ReadCPT (adr, blok, 1, 99))
				 if (alog[blok].checksym)
					{
					 //UL_TRACE((LOGID, "ReadCPT(99) time=%f", (DOUBLE)GetTickCount()-ctime));
					 // если Логика 961, если тип энергоресурса 221, то создается два устройства с типами 2 и 1
					 memcpy (name,"00000000000000",14); strcpy (name,DeviceRU[devNum].name);
					 BOOL newL=FALSE;
					 if (strstr (name,"+")) newL=TRUE;
					 UINT ankorp2=(name[0]-48)*10+(name[1]-48);
					 UINT ankorp=(name[9]-48)*10+(name[10]-48);
					 name[11]=name[9]; name[10]=name[8]; name[9]=name[7]; name[8]=name[6]; name[7]=name[5]; name[6]=name[4]; name[5]='-'; name[4]=name[3]; name[3]=name[2]; name[2]='-';
					 UL_INFO((LOGID,"%s",name));
					 for (UINT i=6;i<=6+MAX_PIPE_NUMS;i++)
						{
						 BOOL ne=FALSE;
						 strcpy (DeviceRU[devNum].name,name);						 
						 switch (name[i])
							{
							 case 48:	UL_INFO((LOGID,"Опана пусто")); break;
							 case 49:	DeviceRU[devNum].type =11; 
										DeviceRU[devNum].pipe=i-5;
										DeviceRU[devNum].tag_num = sizeof(Archive961_2)/sizeof(Archive961_2[0]);
										DeviceRU[devNum].name[6]='2'; 
										DeviceRU[devNum].name[7]=0; 
										ne=TRUE; break;
							 case 50: // 2222
									  if (DeviceRU[devNum].name[i+1]=='2') // 22
										{	// тепло и теплофикационная вода
										 DeviceRU[devNum].type = 12;
										 DeviceRU[devNum].com = blok;
										 DeviceRU[devNum].device = adr;
										 DeviceRU[devNum].speed = speed[blok];
										 DeviceRU[devNum].status = 1;
										 DeviceRU[devNum].pipe=i-5;
										 DeviceRU[devNum].tag_num = sizeof(Archive961_2)/sizeof(Archive961_2[0]);
										 
										 if ((ankorp>0 && name[8]=='4' && i>=9) || (ankorp>0 && name[8]=='2' && i>=8))
											{
											 UL_INFO((LOGID,"ankorp=%d",ankorp));
											 DeviceRU[devNum].pipe=2; // potrebitel
											 DeviceRU[devNum].name[0]=ankorp/10+48;
											 DeviceRU[devNum].name[1]=ankorp%10+48;
											 if (ankorp==ankorp2) DeviceRU[devNum].name[4]=DeviceRU[devNum].name[4]+1;
											}
										 DeviceRU[devNum].name[6]='2';
										 DeviceRU[devNum].name[7]=0;
										 UL_INFO((LOGID,"Device found [%s] on address %d. type=%d. tags=%d pipe=%d", DeviceRU[devNum].name, adr,DeviceRU[devNum].type,DeviceRU[devNum].tag_num,DeviceRU[devNum].pipe));
										 ReadMConf (devNum, adr, DeviceRU[devNum].name, DeviceRU[devNum].tag_num);
										 devNum++; devBus++; i++;
										 										 
										 DeviceRU[devNum].type=11;
										 DeviceRU[devNum].com = blok; 
										 DeviceRU[devNum].device = adr;
										 DeviceRU[devNum].speed = speed[blok];
										 DeviceRU[devNum].status = 1;
										 DeviceRU[devNum].pipe=i-6; // pipe
										 DeviceRU[devNum].tag_num = sizeof(Archive961_1)/sizeof(Archive961_1[0]);
										 DeviceRU[devNum].tp=newL;
										 strcpy (DeviceRU[devNum].name,DeviceRU[devNum-1].name);
									     
										 if ((ankorp>0 && name[8]=='4' && i>=9) || (ankorp>0 && name[8]=='2' && i>=8))
											{
											 UL_INFO((LOGID,"ankorp=%d",ankorp));
											 DeviceRU[devNum].name[0]=ankorp/10+48;
											 DeviceRU[devNum].name[1]=ankorp%10+48;
											}
										 DeviceRU[devNum].name[6]='1'; DeviceRU[devNum].name[7]=0;
										 UL_INFO((LOGID,"Device found [%s.%d] on address %d. type=%d. tags=%d pipe=%d", DeviceRU[devNum].name, DeviceRU[devNum].tp, adr,DeviceRU[devNum].type,DeviceRU[devNum].tag_num,DeviceRU[devNum].pipe)); 
										 ReadMConf (devNum, adr, DeviceRU[devNum].name, DeviceRU[devNum].tag_num);
										 devNum++; devBus++;
										}
									 else
										{	// просто тепло
										 DeviceRU[devNum].type=12;
										 DeviceRU[devNum].pipe=i-5;
										 DeviceRU[devNum].tag_num = sizeof(Archive961_2)/sizeof(Archive961_2[0]);
										 DeviceRU[devNum].name[6]='2'; 
										 DeviceRU[devNum].name[7]=0;
										 ne=TRUE;
										}
									  break;
							 case 51: break;
							 case 52: DeviceRU[devNum].type =13; 
									  DeviceRU[devNum].pipe=i-5;
								      DeviceRU[devNum].tag_num = sizeof(Archive961_4)/sizeof(Archive961_4[0]);
									  DeviceRU[devNum].tp=newL;
									  if (ankorp>0 && i>=9)
										{
										 UL_INFO((LOGID,"ankorp=%d",ankorp));
										 DeviceRU[devNum].name[0]=ankorp/10+48;
										 DeviceRU[devNum].name[1]=ankorp%10+48;
										}
									  if (i>6 && DeviceRU[devNum-1].name[6]=='3') // prev water too
										 DeviceRU[devNum].name[4]=DeviceRU[devNum].name[4]+2;
									  DeviceRU[devNum].name[6]='3'; DeviceRU[devNum].name[7]=0; ne=TRUE; break;
							 case 53: DeviceRU[devNum].type =15; DeviceRU[devNum].pipe=i-5;
								     DeviceRU[devNum].tag_num = sizeof(Archive761)/sizeof(Archive761[0]);
									 DeviceRU[devNum].name[6]='5'; DeviceRU[devNum].name[7]=0; ne=TRUE; break; 
							 case 54: DeviceRU[devNum].type =16; DeviceRU[devNum].pipe=i-5;
								     DeviceRU[devNum].tag_num = sizeof(Archive762_6)/sizeof(Archive762_6[0]);
									 DeviceRU[devNum].name[6]='6'; DeviceRU[devNum].name[7]=0; ne=TRUE; break; 
 							 case 55: DeviceRU[devNum].type =17; DeviceRU[devNum].pipe=i-5;
								     DeviceRU[devNum].tag_num = sizeof(Archive762_7)/sizeof(Archive762_7[0]);
									 DeviceRU[devNum].name[6]='7'; DeviceRU[devNum].name[7]=0; ne=TRUE; break;
							 case 56: break;
							}
						 if (ne)
							{
							 DeviceRU[devNum].com = blok; DeviceRU[devNum].device = adr;
							 DeviceRU[devNum].speed = speed[blok]; DeviceRU[devNum].status = 1;
							 UL_INFO((LOGID,"[%d] Device found [%s] on address %d. type=%d. tags=%d pipe=%d",blok, DeviceRU[devNum].name, adr,DeviceRU[devNum].type,DeviceRU[devNum].tag_num,DeviceRU[devNum].pipe)); 
							 ReadMConf (devNum, adr, DeviceRU[devNum].name, DeviceRU[devNum].tag_num);
							 devNum++; devBus++; ne=FALSE;
							}
						}
					}
				 else UL_INFO((LOGID,"wrong checksym"));
			// else UL_INFO((LOGID,"read failed"));
			}
		}
	}
 if (!devBus) devBus+=tScanBus(blok);
 LogikNum = LogikNum + devBus;
 ReadEndFlag[blok]=TRUE;
 UL_INFO((LOGID,"Total %d logiks found on port Com%d time=%f",devBus,com_num[blok],(DOUBLE) GetTickCount()-etime));
 return;
}
//----------------------------------------------------------------------------------------------
VOID poll_device()
{
 FILETIME ft;
 INT ecode=0;
 DWORD start=0;
 //hThrd[0] = CreateThread (NULL, 0, (LPTHREAD_START_ROUTINE) PollDeviceEtherNet, &hReadEndEvents[0], 0, &dwThrdID); ReadEndFlag[0]=false;
 for (UINT i=0; i<Com_number; i++)
	{
	 hThrd[i+1] = CreateThread (NULL, 0, (LPTHREAD_START_ROUTINE) PollDeviceCOM, (LPVOID)i, 0, &dwThrdID);
	 ReadEndFlag[i+1]=false;
	 Sleep (1000);
	}
 ecode=Com_number; start=GetTickCount();
 while (1)
	{
	 for (UINT i=1; i<=Com_number; i++)
		 if (ReadEndFlag[i]) 
			{
			 ecode--;
			 UL_DEBUG((LOGID, "[ReadEndFlag]=0x%x | i=%d | ecode=%d",ReadEndFlag,i,ecode));
			 ReadEndFlag[i]=false;
			 //UL_DEBUG((LOGID, "exit finished"));
			}
	 if (!my_CF.in_use())
		{
		 UL_DEBUG((LOGID, "my_CF.in_use(). server not use"));
		 for (UINT i=0; i<=Com_number; i++)
			{
			 hReadEndEvents[i]=(HANDLE)0x10;	// exit signal
			 Sleep(5000);
			 return;
			}
		}
	 if (!ecode) 
		{
		 UL_DEBUG((LOGID, "All threads are complete. Total %d seconds.",(GetTickCount()-start)/1000));
		 Sleep(3000);
		 break;
		}
	 Sleep(2000);
	}
 Sleep (200);
 GetSystemTimeAsFileTime(&ft);
 EnterCriticalSection(&lk_values);
 UL_DEBUG((LOGID, "Data to tag (%d)",tag_num));
 for (UINT ci=0;ci<tag_num; ci++)
	{
	 UL_DEBUG((LOGID, "[%d] ci = %d | v = %s",DR[ci].type,ci,DR[ci].value));
	 VARTYPE tvVt = tv[ci].tvValue.vt;
	 VariantClear(&tv[ci].tvValue);	  
	 if (!DR[ci].type)
		{
		 CHAR   *stopstring;
		 V_R4(&tv[ci].tvValue) = (FLOAT) strtod(DR[ci].value, &stopstring);
		 //UL_DEBUG((LOGID, "float %s = %d",DR[ci].value, strlen (DR[ci].value)));
		}
	 else
		{
		 SysFreeString (bufMB);  bufMB[0]=0;
		 if (strlen (DR[ci].value)>1000) UL_DEBUG((LOGID, "DR[ci].value string lenght %d",strlen (DR[ci].value)));
		 if (strlen(DR[ci].value)>1 && DR[ci].value[0]!=0) 
			{
			 //UL_DEBUG((LOGID, "MAKELCID(0x0409, SORT_DEFAULT)"));
			 LCID lcid = MAKELCID(0x0409, SORT_DEFAULT); // This macro creates a locale identifier from a language identifier. Specifies how dates, times, and currencies are formatted
			 //UL_DEBUG((LOGID, "MultiByteToWideChar()"));
	 	 	 MultiByteToWideChar(CP_ACP,	// ANSI code page
									  0,	// flags
						   DR[ci].value,	// points to the character string to be converted
				 strlen(DR[ci].value)+1,	// size in bytes of the string pointed to 
									bufMB,	// Points to a buffer that receives the translated string
			 sizeof(bufMB)/sizeof(bufMB[0]));	// function maps a character string to a wide-character (Unicode) string
	 		 //UL_DEBUG((LOGID, "string %ls",bufMB));
			}		 
		 V_BSTR(&tv[ci].tvValue) = SysAllocString(bufMB);
		 if (wcslen (bufMB)>1000) UL_DEBUG((LOGID, "string lenght %d",wcslen (bufMB)));
		}
	 V_VT(&tv[ci].tvValue) = tvVt;
	 if (DR[ci].status) tv[ci].tvState.tsQuality = OPC_QUALITY_GOOD;
	 else tv[ci].tvState.tsQuality = OPC_QUALITY_UNCERTAIN;
	 if (DR[ci].status==4 || DR[ci].status==5) tv[ci].tvState.tsQuality = OPC_QUALITY_CONFIG_ERROR;
	 tv[ci].tvState.tsTime = ft;
	}
 loCacheUpdate(my_service, tag_num, tv, 0);
 LeaveCriticalSection(&lk_values);
 Sleep(100);
}
//-----------------------------------------------------------------------------------
VOID PollDeviceCOM (LPVOID lpParam)
{
 UL_INFO((LOGID,"PollDeviceCOM (%d)",(UINT) lpParam)); 
 CHAR sBuf2[1000]; CHAR sBuf1[5000]; CHAR Out[500]; CHAR query[1000]; CHAR dataset[500];
 sBuf2[999]=0; sBuf1[4999]=0; Out[499]=0;
 UINT ccom=(UINT)lpParam; UINT timers7=timers[7]; UINT DeviceErr=0; UINT prevadr=0;
 SHORT adres=0;
 UCHAR func941=0;
 SPrint Msp; BOOL arch=FALSE; UINT pip=0;
 UL_INFO((LOGID,"Read COM %d",ccom));
 
 for (UINT device=0;device<devNum;device++)	 
 if (DeviceRU[device].com==(INT)ccom)
 if (DeviceRU[device].type>10)
	{	
	 DeviceErr=0;
	 //UL_INFO((LOGID,"Read device %d . type=%d . pipe=%d",device, DeviceRU[device].type,DeviceRU[device].pipe)); 	 
	 UL_DEBUG((LOGID, "[com=%d/ln=%d] device=%d",DeviceRU[device].com,DeviceRU[device].device,device));

	 if (DeviceRU[device].type>20) ReadCPT (DeviceRU[device].device, DeviceRU[device].com, 0, 0);
	 Sleep (1000);
 	 UINT i=0,max_t=1;
	 SYSTEMTIME lpST; FILETIME lpFT; ULARGE_INTEGER ui,uo;
	 DWORD dwStoredFlags = EV_RXCHAR | EV_TXEMPTY | EV_RXFLAG;
	 port[DeviceRU[device].com].SetMask (dwStoredFlags);
	 GetLocalTime(&lpST); SystemTimeToFileTime(&lpST,&lpFT);
	 ui.QuadPart= lpFT.dwLowDateTime + lpFT.dwHighDateTime * 0x100000000L;
	 Msp.end = lpFT;
	 for (UINT pdr=1; pdr<=(UINT)DeviceRU[device].tag_num; pdr++)
		{
		 //UL_DEBUG((LOGID, "[com=%d/ln=%d] device=%d | pdr=%d | ",DeviceRU[device].com,DeviceRU[device].device,device,pdr));
		 if (!my_CF.in_use()) { UL_INFO((LOGID,"exit signal")); return;}		 

		 Msp.pipe = DeviceRU[device].pipe; pip=Msp.pipe;
 		 if (DeviceRU[device].type==11 || DeviceRU[device].type==21) if (strstr (Archive961_1[pdr-1].name,"подающей")) pip=1+(DeviceRU[device].pipe/3)*2+DeviceRU[device].pipe/4;
		 if (DeviceRU[device].type==11 || DeviceRU[device].type==21) if (strstr (Archive961_1[pdr-1].name,"обратной")) pip=2+(DeviceRU[device].pipe/3)*2+DeviceRU[device].pipe/4;
		 
		 switch (DeviceRU[device].type)
			{
			 case 11: if (Archive961_1[pdr-1].type) arch = TRUE; else arch = FALSE; if (Archive961_1[pdr-1].type>10) pip=0; break;
			 case 12: if (Archive961_2[pdr-1].type) arch = TRUE; else arch = FALSE; if (Archive961_2[pdr-1].type>10) pip=0; break;
			 case 13: if (Archive961_4[pdr-1].type) arch = TRUE; else arch = FALSE; if (Archive961_4[pdr-1].type>10) pip=0; break;
			 case 15: if (Archive761[pdr-1].type)	arch = TRUE; else arch = FALSE; if (Archive761[pdr-1].type>10) pip=0;	break;
			 case 16: if (Archive762_6[pdr-1].type) arch = TRUE; else arch = FALSE; if (Archive762_6[pdr-1].type>10) pip=0; break;
			 case 17: if (Archive762_7[pdr-1].type) arch = TRUE; else arch = FALSE; if (Archive762_7[pdr-1].type>10) pip=0; break;
			 
			 case 21: if (Archive941_1[pdr-1].type) arch = TRUE; else arch = FALSE; if (Archive941_1[pdr-1].type>10) pip=0; break;
			 case 22: if (Archive941_2[pdr-1].type) arch = TRUE; else arch = FALSE; if (Archive941_2[pdr-1].type>10) pip=0; break;
			 case 24: if (Archive941_4[pdr-1].type) arch = TRUE; else arch = FALSE; if (Archive941_4[pdr-1].type>10) pip=0; break;
			}
		 Msp.pipe=pip;
		 
		 //UL_DEBUG((LOGID, "port %d(%d) <%d/%d> <%d,%d>",DeviceRU[device].com,arch,pdr,DeviceRU[device].tag_num,timers[6],timers[7]));
		 if (!arch)
			{
			 //UL_DEBUG((LOGID, "[com=%d/ln=%d] device=%d | pdr=%d | type=%d",DeviceRU[device].com,DeviceRU[device].device,device,pdr,arch));
			 if (timers[0]) continue;
			 switch (DeviceRU[device].type)
				{
			     case 11: if (DeviceRU[device].tp) { adres=Archive961M_1[pdr-1].adr;  strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,Archive961M_1[pdr-1].adr, pip, Msp),100); }
						  else { adres=Archive961_1[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,Archive961_1[pdr-1].adr, pip, Msp),100); }
						  break; // 2
				 case 12: adres=Archive961_2[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,Archive961_2[pdr-1].adr, pip, Msp),100); break; // 1
				 case 13: if (DeviceRU[device].tp) { adres=Archive961M_4[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,Archive961M_4[pdr-1].adr, pip, Msp),100); }
						  else { adres=Archive961_4[pdr-1].adr;strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,Archive961_4[pdr-1].adr, pip, Msp),100);	}
						  break; // 3
				 case 15: adres=Archive761[pdr-1].adr;	 strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,Archive761[pdr-1].adr, pip, Msp),100); break;
				 case 16: adres=Archive762_6[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,Archive762_6[pdr-1].adr, pip, Msp),100); break;
				 case 17: adres=Archive762_7[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,Archive762_7[pdr-1].adr, pip, Msp),100); break;

				 case 21: memcpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x52,0,Archive941_1[pdr-1].adr, pip, Msp),10); Out[100]=0; break;
				 case 22: memcpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x52,0,Archive941_2[pdr-1].adr, pip, Msp),10); Out[100]=0; break;
				 case 24: memcpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x52,0,Archive941_4[pdr-1].adr+(DeviceRU[device].pipe-1)*4, pip, Msp),10); Out[100]=0; break;
				}
			 //UL_DEBUG((LOGID, "[com=%d/ln=%d] device=%d | pdr=%d | type=%d | formseq=%d",DeviceRU[device].com,DeviceRU[device].device,device,pdr,arch,strlen(Out)));
			 max_t=3;
			}
		 else 
			{
			 //UL_DEBUG((LOGID, "[com=%d/ln=%d] device=%d | pdr=%d | type=%d",DeviceRU[device].com,DeviceRU[device].device,device,pdr,arch));
			 switch (DeviceRU[device].type)
				{
				 case 11: Msp.type = Archive961_1[pdr-1].type;	Msp.quant = Archive961_1[pdr-1].quant;	break;
				 case 12: Msp.type = Archive961_2[pdr-1].type;	Msp.quant = Archive961_2[pdr-1].quant;	break;
				 case 13: Msp.type = Archive961_4[pdr-1].type;	Msp.quant = Archive961_4[pdr-1].quant;	break;
				 case 15: Msp.type = Archive761[pdr-1].type;	Msp.quant = Archive761[pdr-1].quant;	break;
				 case 16: Msp.type = Archive762_6[pdr-1].type;	Msp.quant = Archive762_6[pdr-1].quant;	break;
				 case 17: Msp.type = Archive762_7[pdr-1].type;	Msp.quant = Archive762_7[pdr-1].quant;	break;

				 case 21: Msp.type = Archive941_1[pdr-1].type;	Msp.quant = Archive941_1[pdr-1].quant;	break;
				 case 22: Msp.type = Archive941_2[pdr-1].type;	Msp.quant = Archive941_2[pdr-1].quant;	break;
				 case 24: Msp.type = Archive941_4[pdr-1].type;	Msp.quant = Archive941_4[pdr-1].quant;	break;
				}
			 if (timers[Msp.type]) continue;
			 
			 if (DeviceRU[device].type<20)
				{
				 ui.QuadPart= lpFT.dwLowDateTime + lpFT.dwHighDateTime * 0x100000000L;
				 switch (Msp.type)
					{
					 case 1: if (timers[6]%2==1) ui.QuadPart = ui.QuadPart - 3*36000000000L; 
							 else 
								{
								 uo.QuadPart = ui.QuadPart - timers[6]*5*36000000000L+36000000000L;
			 					 Msp.end.dwLowDateTime=uo.LowPart;
								 Msp.end.dwHighDateTime=uo.HighPart;
								 ui.QuadPart = ui.QuadPart - (timers[6]+1)*5*36000000000L-36000000000L;
								} break;	// за час (последняя часовая метка)
					 case 2: if (timers[7]>5) ui.QuadPart = ui.QuadPart - 5*24*36000000000L;
							 else 
								{
 								 uo.QuadPart = ui.QuadPart - timers[7]*3*24*36000000000L + 24*36000000000L;
				 				 Msp.end.dwLowDateTime=uo.LowPart;
								 Msp.end.dwHighDateTime=uo.HighPart;
								 ui.QuadPart = ui.QuadPart - (timers[7]+1)*3*24*36000000000L - 24*36000000000L;
								} break;	 // за сутки 
					 case 3: ui.QuadPart = ui.QuadPart - 3*10*24*36000000000L; break; // за декаду
					 case 4: ui.QuadPart = ui.QuadPart - 3*31*24*36000000000L; break; // за месяц
					 case 5: ui.QuadPart = ui.QuadPart - 3*8*36000000000L; break; // за смену
					 case 11:ui.QuadPart = ui.QuadPart - 6*24*8*36000000000L; break; 
					 case 12:ui.QuadPart = ui.QuadPart - 3*24*36000000000L; break; 
					}
				 Msp.start.dwLowDateTime=ui.LowPart;
				 Msp.start.dwHighDateTime=ui.HighPart;
				 max_t=5;
				}
			 else
				{
				 ui.QuadPart= lpFT.dwLowDateTime + lpFT.dwHighDateTime * 0x100000000L;
				 switch (Msp.type)
					{ 
					 case 1: ui.QuadPart = ui.QuadPart - timers[6]*36000000000L; func941=0x48; break;		// за час
					 case 2: ui.QuadPart = ui.QuadPart - timers7*24*36000000000L; func941=0x59; break;	// за сутки
					 case 4: ui.QuadPart = ui.QuadPart - timers7*31*24*36000000000L; func941=0x4D; break;			// за месяц
					 case 12: pip=0x189+(timers[7]); func941=0x45; break;
					}
				 Msp.start.dwLowDateTime=ui.LowPart;				 
				 Msp.start.dwHighDateTime=ui.HighPart;
				 max_t=2;
				}
			 
			 switch (DeviceRU[device].type)
				{
				 case 11: adres=Archive961_1[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0xe,0,Archive961_1[pdr-1].adr,pip,Msp),100); break;
				 case 12: adres=Archive961_2[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0xe,0,Archive961_2[pdr-1].adr,pip,Msp),100); break;
				 case 13: adres=Archive961_4[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0xe,0,Archive961_4[pdr-1].adr,pip,Msp),100); break;
				 case 15: adres=Archive761[pdr-1].adr;	 strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0xe,0,Archive761[pdr-1].adr,pip,Msp),100);	break;
				 case 16: adres=Archive762_6[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0xe,0,Archive762_6[pdr-1].adr,pip,Msp),100); break;
				 case 17: adres=Archive762_7[pdr-1].adr; strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0xe,0,Archive762_7[pdr-1].adr,pip,Msp),100); break;

				 case 21: memcpy (Out,FormSequence((UCHAR)DeviceRU[device].device,func941,0,Archive941_1[pdr-1].adr,pip,Msp),10); Out[100]=0; break;
				 case 22: memcpy (Out,FormSequence((UCHAR)DeviceRU[device].device,func941,0,Archive941_2[pdr-1].adr,pip,Msp),10); Out[100]=0; break;
				 case 24: memcpy (Out,FormSequence((UCHAR)DeviceRU[device].device,func941,0,Archive941_4[pdr-1].adr,pip,Msp),10); Out[100]=0; break;
				}
			 if (pip>=0x189) memcpy (Out,FormSequence((UCHAR)DeviceRU[device].device,func941,0,pip,88,Msp),10); 
			 //UL_DEBUG((LOGID, "[com=%d/ln=%d] device=%d | pdr=%d | type=%d | formseq=%d",DeviceRU[device].com,DeviceRU[device].device,device,pdr,arch,strlen(Out)));			 
			}			 
		 
		 //UL_INFO((LOGID,"port=%d",DeviceRU[device].com));		 
		 UL_DEBUG((LOGID, "[%d][0x%x][com=%d/ln=%d] device=%d | pdr=%d",(UINT)lpParam,port[DeviceRU[device].com].ReadHandle(),DeviceRU[device].com,DeviceRU[device].device,device,pdr));
	     //if ((UINT)lpParam!=DeviceRU[device].com) UL_INFO((LOGID,"ERROR !!!!! port=%d | %d",DeviceRU[device].com,(UINT)lpParam)); 
		 DWORD dwErrors=CE_FRAME|CE_IOE|CE_TXFULL|CE_RXPARITY|CE_RXOVER|CE_OVERRUN|CE_MODE|CE_BREAK;
	
		 port[DeviceRU[device].com].ClearWriteBuffer();
		 port[DeviceRU[device].com].Purge(PURGE_TXABORT|PURGE_RXABORT|PURGE_TXCLEAR|PURGE_RXCLEAR);
		 port[DeviceRU[device].com].ClearError(dwErrors);
		 port[DeviceRU[device].com].Read(sBuf1, 100);

		 UINT len=9; 
		 if (DeviceRU[device].type<20) len=strlen(Out);
		 if (Out[len]==0) { Out[len+1]=0; len++; } 
		 for (UINT po=0;po<=len;po++)
			{
			 port[DeviceRU[device].com].Write (Out+po, 1);
			 //if (DeviceRU[device].com==5 && DeviceRU[device].device==4 && (pdr==2 || pdr==3)) 				UL_INFO((LOGID,"O[%d] = 0x%x",po,(UCHAR)Out[po]));
			 //port[DeviceRU[device].com].WaitEvent (dwStoredFlags);
			}
		 Sleep (1850);
		 if (arch) Sleep (2500);		 
	
		 DWORD ctime=GetTickCount();
		 DWORD dwBytesRead=0, enddt=0;
		 DWORD dwbr1=dwBytesRead;
		 COMSTAT stat;
		 //while ((GetTickCount()-ctime)<(max_t*1100))
		 for (UINT rr=0; rr<max_t*10; rr++)
			{
			 port[DeviceRU[device].com].GetStatus(stat);
			 if (stat.cbInQue)
				{
				 dwBytesRead = port[DeviceRU[device].com].Read(sBuf2, 70);
				 //UL_INFO((LOGID,"[0x%x]",GetLastError()));
				 if (dwBytesRead!=-1 && dwbr1<4900) for (UINT i=0;i<dwBytesRead;i++) sBuf1[dwbr1+i]=sBuf2[i];
				 dwbr1=dwbr1+dwBytesRead;
				 enddt=50;
				}
			 else enddt++;
			 if (enddt>55) break;
			 Sleep (50);
			}
		 if (dwbr1==0) DeviceErr++; 
		 else 
			{			 
 			 if (DeviceErr) 
				{
				 sprintf(query,"UPDATE uzel SET conn='1',date=date WHERE idkon='%c%c_%c%c_%c'",DeviceRU[device].name[0],DeviceRU[device].name[1],DeviceRU[device].name[3],DeviceRU[device].name[4],DeviceRU[device].name[6]);
				 //UL_DEBUG((LOGID, "[%s]",query));				 
				 dbase.sqlexec((UCHAR FAR *) query, dataset);
				}
			 DeviceErr=0;
			}
		 if (DeviceErr>3) 
			{			 
			 UL_INFO((LOGID,"device [%d (com%d/%s)] not answer [0x%x]",device,DeviceRU[device].com,DeviceRU[device].name,GetLastError()));
			 pdr=(UINT)DeviceRU[device].tag_num;
			 sprintf(query,"UPDATE uzel SET conn='0',date=date WHERE idkon='%c%c_%c%c_%c'",DeviceRU[device].name[0],DeviceRU[device].name[1],DeviceRU[device].name[3],DeviceRU[device].name[4],DeviceRU[device].name[6]);
			 //UL_DEBUG((LOGID, "[%s]",query));
			 dbase.sqlexec((UCHAR FAR *) query, dataset);
			}
		 //if (DeviceRU[device].com==5 && DeviceRU[device].device==4 && (pdr==2 || pdr==3)) 			 UL_DEBUG((LOGID, "[com=%d/ln=%d] device=%d | pdr=%d | type=%d | read=%d",DeviceRU[device].com,DeviceRU[device].device,device,pdr,arch,dwbr1));
		 // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		 //DWORD dwbr1=0;
		 //sBuf1[0]=0;
		 //if (DeviceRU[device].com==5 && DeviceRU[device].device==4 && (pdr==2 || pdr==3)) 			for (UINT i=0;i<dwbr1;i++) UL_INFO((LOGID,"[%d][%d]  [%d] = 0x%x [%c]",device,pdr,i,(UCHAR)sBuf1[i],(UCHAR)sBuf1[i]));

		 if (DeviceRU[device].type<20)
			{
			 alog[DeviceRU[device].com].npAdr=0;
			 BOOL AS=FALSE; 
			 //UL_DEBUG((LOGID, "[com=%d/ln=%d] AnalyseSequence",DeviceRU[device].com,DeviceRU[device].device));
			 if (dwbr1>0) AS=AnalyseSequence ((CHAR*)sBuf1,dwbr1,DeviceRU[device].com);
			 //UL_DEBUG((LOGID, "[com=%d/ln=%d] device=%d | pdr=%d |type = %d / alog=%s / quant=%d",DeviceRU[device].com,DeviceRU[device].device,device,pdr,DR[rr].type,alog[DeviceRU[device].com].data[0],alog[DeviceRU[device].com].quant_param));
		     //UL_DEBUG((LOGID, "[com=%d/ln=%d] AS=%d | npAdr=%d adres=%d | checksym = %d",DeviceRU[device].com,DeviceRU[device].device,AS,alog[DeviceRU[device].com].npAdr,adres,alog[DeviceRU[device].com].checksym));
			 if (AS)
			 if (alog[DeviceRU[device].com].npAdr == adres)
			 if (alog[DeviceRU[device].com].checksym)
			 for (UINT rr=0;rr<tag_num;rr++)
				{
				 //UL_DEBUG((LOGID, "[%d] %d=%d %d=%d (%d)",rr,DR[rr].kanal,pdr,DR[rr].device,device,alog[DeviceRU[device].com].checksym));
				 if ((UINT)DR[rr].kanal==pdr && (UINT)DR[rr].device==device)
					{					 
					 DR[rr].status = 1;
					 DR[rr].value[0]=0;
					 UL_DEBUG((LOGID, "[com=%d/ln=%d] type = %d[%d] / alog=%s / quant=%d",DeviceRU[device].com,DeviceRU[device].device,DR[rr].type,alog[DeviceRU[device].com].npAdr,alog[DeviceRU[device].com].data[0],alog[DeviceRU[device].com].quant_param));
					 if (DR[rr].type==0)
						{
	 	  				 for (UINT p=0;p<strlen(alog[DeviceRU[device].com].data[0]);p++)
						 	if (*(alog[DeviceRU[device].com].data[0]+p)=='.')
						 		*(alog[DeviceRU[device].com].data[0]+p)=',';
						 strcpy (DR[rr].value,alog[DeviceRU[device].com].data[0]);
						}
					 if (DR[rr].type==1)
						{
						 strcpy (DR[rr].value,"");
						 if (alog[DeviceRU[device].com].quant_param && (pip == alog[DeviceRU[device].com].pipe))
						 for (UINT j=0; j<alog[DeviceRU[device].com].quant_param; j++)
							{
							 //UL_DEBUG((LOGID, "%s|%s",alog[DeviceRU[device].com].time[j],alog[DeviceRU[device].com].data[j]));
							 if (strlen (alog[DeviceRU[device].com].data[j])>50) break;
							 if (strlen (alog[DeviceRU[device].com].type[j])>50) break;
							 if (strlen (alog[DeviceRU[device].com].time[j])>50) break;
							 for (UINT oo=0;oo<strlen (alog[DeviceRU[device].com].time[j]);oo++) if (alog[DeviceRU[device].com].time[j][oo]==0x20) alog[DeviceRU[device].com].time[j][oo]=0x30;
							 if (strlen (alog[DeviceRU[device].com].type[j])>11)
								sprintf (DR[rr].value,"%s(%s)(%s)(%s)",DR[rr].value,alog[DeviceRU[device].com].time[j],alog[DeviceRU[device].com].type[j],alog[DeviceRU[device].com].data[j]);
							 else
								sprintf (DR[rr].value,"%s(%s)(%s)",DR[rr].value,alog[DeviceRU[device].com].time[j],alog[DeviceRU[device].com].data[j]);
							 if(strlen(DR[rr].value)>450) j=1000;
							}
						 if (pip != alog[DeviceRU[device].com].pipe)
							{
							 Sleep (30);
							// port[DeviceRU[device].com].ClearReadBuffer();
							 //port[DeviceRU[device].com].Purge(PURGE_TXABORT|PURGE_RXABORT|PURGE_TXCLEAR|PURGE_RXCLEAR);
							 //port[DeviceRU[device].com].Read(sBuf1, 100);							 
							}
						 if (strlen (DR[rr].value)) UL_DEBUG((LOGID, "[%d][%d][%d](%d) DR=%s",pip,alog[DeviceRU[device].com].pipe,pdr,dwbr1,DR[rr].value));
						}					 
					 if (strstr(alog[DeviceRU[device].com].data[0],"Дата/время")) 
						{
						 DR[rr].status = 4;
						 strcpy (DR[rr].value,"0");
						}
					 if (strstr(alog[DeviceRU[device].com].data[0],"Пара")) 
						{
						 DR[rr].status = 4;
						 strcpy (DR[rr].value,"0");
						}
					 if (strstr(alog[DeviceRU[device].com].data[0],"Нет") || strstr(alog[DeviceRU[device].com].data[0],"испол")) 
						{
						 DR[rr].status = 5;
						 strcpy (DR[rr].value,"0");
						}
					 //UL_DEBUG((LOGID, "%s [%d]",DR[rr].value,DR[rr].status));
					 rr=tag_num+1;
					}
				 else DR[rr].status = 1;
				}
			 Sleep (100);
			}
		 else
			{
			 BYTE BData[10];
			 if (dwbr1>0) if (AnalyseAnswer ((CHAR*)sBuf1,dwbr1,DeviceRU[device].com,pip))
			 if (alog[DeviceRU[device].com].checksym)
			 for (UINT rr=0;rr<tag_num;rr++)
				{
				 if ((UINT)DR[rr].kanal==pdr && (UINT)DR[rr].device==device)
					{
					 DR[rr].status = 1;
					 DR[rr].value[0]=0;
					 //UL_DEBUG((LOGID, "[com=%d/ln=%d] type = %d / alog=%s / quant=%d",DeviceRU[device].com,DeviceRU[device].device,DR[rr].type,DR[rr].value,alog[DeviceRU[device].com].quant_param));
					 if (DR[rr].type==0)
						{
 						 BData[0]=alog[DeviceRU[device].com].data[0][3];
						 BData[1]=alog[DeviceRU[device].com].data[0][2];
						 BData[2]=alog[DeviceRU[device].com].data[0][1];
						 BData[3]=alog[DeviceRU[device].com].data[0][0];
						 DOUBLE res = cIEEE754toFloat (BData);						 
						 if (res<200000 && res>-100) sprintf (DR[rr].value,"%f",res);						 
						 //UL_DEBUG((LOGID, " 0x%x 0x%x 0x%x 0x%x = %s",BData[0],BData[1],BData[2],BData[3],DR[rr].value));
						 DR[rr].status = 1;
						}
					 if (DR[rr].type==1)
						{
						 DR[rr].status = 1;
						 SHORT hr=0; CHAR ed[10];
						 SYSTEMTIME stb;
						 FileTimeToSystemTime(&Msp.start,&stb);
						 if (alog[DeviceRU[device].com].func==0x48) { stb.wSecond=0; stb.wMinute=0;}
						 if (alog[DeviceRU[device].com].func==0x59) { stb.wHour=0; stb.wSecond=0; stb.wMinute=0;}
						 if (alog[DeviceRU[device].com].func==0x4D) { stb.wDay=1; stb.wHour=0; stb.wSecond=0; stb.wMinute=0;}						 
						 switch (DeviceRU[device].type)
							{
							 case 21: hr=Archive941_1[pdr-1].adr/4+(DeviceRU[device].pipe-1); strcpy (ed,Archive941_1[pdr-1].meas); break;
							 case 22: hr=Archive941_2[pdr-1].adr/4+(DeviceRU[device].pipe-1); strcpy (ed,Archive941_2[pdr-1].meas); break;
							 case 24: hr=Archive941_4[pdr-1].adr/4+(DeviceRU[device].pipe-1); strcpy (ed,Archive941_4[pdr-1].meas); break;
							}
						 if (pip<0x189 && alog[DeviceRU[device].com].quant_param)
							{
 							 BData[0]=alog[DeviceRU[device].com].data[hr][3];
							 BData[1]=alog[DeviceRU[device].com].data[hr][2];
							 BData[2]=alog[DeviceRU[device].com].data[hr][1];
							 BData[3]=alog[DeviceRU[device].com].data[hr][0];
							 DOUBLE res = cIEEE754toFloat (BData);
							 //UL_DEBUG((LOGID, "[%d][%d] 0x%x 0x%x 0x%x 0x%x = %f",hr,DeviceRU[device].pipe,BData[0],BData[1],BData[2],BData[3],res));
							 //sprintf (DR[rr].value,"(%f)(%s)(%0.2d-%0.2d-%0.2d/%0.2d:%0.2d:%0.2d)",res,ed,stb.wDay,stb.wMonth,stb.wYear,stb.wHour,stb.wMinute,stb.wSecond);
							 if (res>1000000 || res<-1000)	;
							 else sprintf (DR[rr].value,"(%0.2d-%0.2d-%0.2d/%0.2d:%0.2d:%0.2d)(%f)",stb.wDay,stb.wMonth,stb.wYear,stb.wHour,stb.wMinute,stb.wSecond,res);
							 //UL_DEBUG((LOGID, "DR[rr].value=%s",DR[rr].value));
							}
						 else
							{
							 sprintf (DR[rr].value,"");
							 for (UINT u=0;u<25;u++)
								for (UINT j=0;j<64;j++)							 
									{
									 sBuf1[u*64+j]=alog[DeviceRU[device].com].data[u][j];
									 //UL_DEBUG((LOGID, "sBuf1[%d]=0x%x",u*64+j,sBuf1[u*64+j]));
									}
							 for (u=0;u<(64*25)-32;u++)
								{
								 if (sBuf1[u]==0x10)
									{
									 // 10 06 06 13 05 10 0F 01
									 // (14-01-97/04:08:3)(+т3-05-01)
									 sprintf (DR[rr].value,"%s(%02d-%02d-%d/%02d:%02d:00)([%d] %d)",DR[rr].value,sBuf1[u+3],sBuf1[u+2],2000+(UCHAR)sBuf1[u+1],sBuf1[u+4],sBuf1[u+5],sBuf1[u+7],sBuf1[u+6]);
									 u+=8;
									}
								}
							 //UL_DEBUG((LOGID, "NS=%s",DR[rr].value));
							}
						}
					 rr=tag_num+1;
					}
				}
			} 
		}
	 if (DeviceRU[device].type>10)
		{
		 SYSTEMTIME sts,curr;
		 FILETIME ftt; SHORT ks;
		 GetLocalTime (&curr);
		 GetLocalTime (&sts);
		 // проверка времени 	 
		 // если 1 апреля и время меньше 10 минут (30.03.2007 15:00)
		 if (curr.wDay==1 && curr.wMonth==4 && curr.wHour==2 && curr.wMinute<20)
			{		 
			 // на зимнее время переходят обычно в 3:00 последнего воскресенья октября -1 час
			 sts.wSecond=0; sts.wMonth=10; sts.wHour=3; sts.wMinute=0; sts.wDay=31;
			 SystemTimeToFileTime(&sts,&ftt);
			 FileTimeToSystemTime(&ftt,&sts);
			 // 0-6
			 // 10 01 01 80 10 1F 14 53 34 10 02 09 30 09 32 32 09 31 09 31 0C 09 33 30 2D 31 30 2D 30 37 0C 10 03 B0 98
			 sts.wDay=31-sts.wDayOfWeek;
			 sprintf (Out,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c22%c%c%c%c%c%c%02d-%02d-%02d%c%c%c",DLE,SOH,DeviceRU[device].device,0x80,DLE,ISI,0x14,0x53,0x34,DLE,STX,HT,0+48,HT,HT,1+48,HT,1+48,FF,HT,sts.wDay,sts.wMonth,sts.wYear-2000,FF,DLE,ETX);
			 ks = CRCode ((UCHAR*)Out+2, strlen(Out)-2);
			 sprintf (Out,"%s%c%c",Out,(UCHAR)(ks>>8),(UCHAR)(ks%256));
			 WriteToPort (DeviceRU[device].com, device, Out);	// 022н01
			 sprintf (Out,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c22%c%c%c%c%c%c%02d-%02d-%02d%c%c%c",DLE,SOH,DeviceRU[device].device,0x80,DLE,ISI,0x14,0x53,0x34,DLE,STX,HT,0+48,HT,HT,2+48,HT,1+48,FF,HT,3,0,0,FF,DLE,ETX);
			 ks = CRCode ((UCHAR*)Out+2, strlen(Out)-2);
			 sprintf (Out,"%s%c%c",Out,(UCHAR)(ks>>8),(UCHAR)(ks%256));
			 WriteToPort (DeviceRU[device].com, device, Out);	// 022н02			 
			 sprintf (Out,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c22%c%c%c%c%c%c-1%c%c%c",DLE,SOH,DeviceRU[device].device,0x80,DLE,ISI,0x14,0x53,0x34,DLE,STX,HT,0+48,HT,HT,3+48,HT,1+48,FF,HT,FF,DLE,ETX);
			 ks = CRCode ((UCHAR*)Out+2, strlen(Out)-2);
			 sprintf (Out,"%s%c%c",Out,(UCHAR)(ks>>8),(UCHAR)(ks%256));
			 WriteToPort (DeviceRU[device].com, device, Out);	// 022н03
			}
		 if (curr.wDay==1 && curr.wMonth==11 && curr.wHour==2 && curr.wMinute<20)
			{		 
			 // на летнее время в 2:00 последнего воскресенья марта +1 час
			 sts.wSecond=0; sts.wMonth=3; sts.wHour=2; sts.wMinute=0; sts.wDay=31;
			 SystemTimeToFileTime(&sts,&ftt);
			 FileTimeToSystemTime(&ftt,&sts);
			 // 0-6
			 sts.wDay=sts.wDay-sts.wDayOfWeek;			 
			 sprintf (Out,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c22%c%c%c%c%c%c%02d-%02d-%02d%c%c%c",DLE,SOH,DeviceRU[device].device,0x80,DLE,ISI,0x14,0x53,0x34,DLE,STX,HT,0+48,HT,HT,1+48,HT,1+48,FF,HT,sts.wDay,sts.wMonth,sts.wYear-2000,FF,DLE,ETX);
			 ks = CRCode ((UCHAR*)Out+2, strlen(Out)-2);
			 sprintf (Out,"%s%c%c",Out,(UCHAR)(ks>>8),(UCHAR)(ks%256));
			 WriteToPort (DeviceRU[device].com, device, Out);	// 022н01
			 sprintf (Out,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c22%c%c%c%c%c%c%02d-%02d-%02d%c%c%c",DLE,SOH,DeviceRU[device].device,0x80,DLE,ISI,0x14,0x53,0x34,DLE,STX,HT,0+48,HT,HT,2+48,HT,1+48,FF,HT,2,0,0,FF,DLE,ETX);
			 ks = CRCode ((UCHAR*)Out+2, strlen(Out)-2);
			 sprintf (Out,"%s%c%c",Out,(UCHAR)(ks>>8),(UCHAR)(ks%256));
			 WriteToPort (DeviceRU[device].com, device, Out);	// 022н02
			 sprintf (Out,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c22%c%c%c%c%c%c-1%c%c%c",DLE,SOH,DeviceRU[device].device,0x80,DLE,ISI,0x14,0x53,0x34,DLE,STX,HT,0+48,HT,HT,3+48,HT,1+48,FF,HT,1+48,FF,DLE,ETX);
			 ks = CRCode ((UCHAR*)Out+2, strlen(Out)-2);
			 sprintf (Out,"%s%c%c",Out,(UCHAR)(ks>>8),(UCHAR)(ks%256));		 
			 WriteToPort (DeviceRU[device].com, device, Out);	// 022н03
			}
		 // если время 02:00-02:10 то запись в 061 время текущее (30.03.2007 16:00)
		 if (curr.wHour==2 && curr.wMinute<10)
			{
			 sprintf (Out,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c21%c%c%02d-%02d-%02d%c%c%c",DLE,SOH,DeviceRU[device].device,0x80,DLE,ISI,0x3,0x53,0x34,DLE,STX,HT,0+48,HT,FF,HT,curr.wHour,curr.wMinute,curr.wSecond,FF,DLE,ETX);
			 ks = CRCode ((UCHAR*)Out+2, strlen(Out)-2);
			 sprintf (Out,"%s%c%c",Out,(UCHAR)(ks>>8),(UCHAR)(ks%256));
			 WriteToPort (DeviceRU[device].com, device, Out);	// 022н02
			}		 
		}
	 //if (prevadr!=DeviceRU[device].device)
	 if (1)
		{
		 prevadr=DeviceRU[device].device;
		 pip=0;
		 if (DeviceRU[device].tp) strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0xc,0,54,pip,Msp),100);
		 else strncpy (Out,FormSequence((UCHAR)DeviceRU[device].device,0x1d,0,54,pip,Msp),100);
		 //10 01 01 80 10 1F 1D 53 35 10 02 09 30 09 35 34 0C 10 03 5A 22
		 //10 01 02 80 10 1F 0C 53 33 10 02 09 30 09 35 34 09 30 09 31 0C 10 03 55 A8 	
		 
		 DWORD dwErrors=CE_FRAME|CE_IOE|CE_TXFULL|CE_RXPARITY|CE_RXOVER|CE_OVERRUN|CE_MODE|CE_BREAK;	
		 port[DeviceRU[device].com].ClearWriteBuffer();
		 port[DeviceRU[device].com].Purge(PURGE_TXABORT|PURGE_RXABORT|PURGE_TXCLEAR|PURGE_RXCLEAR);
		 port[DeviceRU[device].com].ClearError(dwErrors);
		 port[DeviceRU[device].com].Read(sBuf1, 100);
		 UINT len=strlen(Out);
		 DWORD dwbr1=0;
		 if (Out[len]==0) { Out[len+1]=0; len++; }
		 for (UINT po=0;po<=len;po++)
			{
			 port[DeviceRU[device].com].Write (Out+po, 1);
			 //UL_INFO((LOGID,"O[%d] = 0x%x",po,(UCHAR)Out[po]));
			}
		 Sleep (2000);
	
		 DWORD dwBytesRead=0, enddt=0;
		 COMSTAT stat; BOOL AS=FALSE; 
		 port[DeviceRU[device].com].GetStatus(stat);
		 if (stat.cbInQue) dwBytesRead = port[DeviceRU[device].com].Read(sBuf1, 70);
		 //if (DeviceRU[device].tp) for (UINT i=0;i<dwBytesRead;i++) UL_INFO((LOGID,"[%d][54]  [%d] = 0x%x [%c]",device,i,(UCHAR)sBuf1[i],(UCHAR)sBuf1[i]));
		 if (dwBytesRead>0) AS=AnalyseSequence ((CHAR*)sBuf1,dwBytesRead,DeviceRU[device].com);
		 if (AS) if (alog[DeviceRU[device].com].checksym)
			{
			 UL_DEBUG((LOGID, "[%d][%s] > [%d]",DeviceRU[device].pipe,alog[DeviceRU[device].com].data[0],alog[DeviceRU[device].com].data[0][DeviceRU[device].pipe-1]%2));
			 sprintf(query,"UPDATE uzel SET P15='%d',date=date WHERE idkon='%c%c_%c%c_%c'",alog[DeviceRU[device].com].data[0][DeviceRU[device].pipe-1]%2,DeviceRU[device].name[0],DeviceRU[device].name[1],DeviceRU[device].name[3],DeviceRU[device].name[4],DeviceRU[device].name[6]);
			 //UL_DEBUG((LOGID, "[%s]",query));
			 dbase.sqlexec((UCHAR FAR *) query, dataset);
			}
		}
	}
 Sleep (2000);
 ReadEndFlag[ccom+1]=true; 
 UL_INFO((LOGID,"PollDeviceCOM (%d) complete",(UINT) lpParam)); 
 //if (sBuf2[999]!=0) UL_INFO((LOGID,"Memory leaks sBuf2"));
 //delete sBuf2;	// buffer for rs-232
 //if (sBuf1[9999]!=0) UL_INFO((LOGID,"Memory leaks sBuf1")); 
 //delete sBuf1;
 //if (Out[999]!=0) UL_INFO((LOGID,"Memory leaks Out")); 
 //delete Out;
}
//-------------------------------------------------------------------
VOID WriteToPort (UINT com, UINT device, CHAR* Out)
{
 DWORD enddt=0, dwbr1=0;
 COMSTAT stat; CHAR sBuf1[1000];
 DWORD dwErrors=CE_FRAME|CE_IOE|CE_TXFULL|CE_RXPARITY|CE_RXOVER|CE_OVERRUN|CE_MODE|CE_BREAK;
 port[com].ClearWriteBuffer();
 port[com].Purge(PURGE_TXABORT|PURGE_RXABORT|PURGE_TXCLEAR|PURGE_RXCLEAR);
 port[com].ClearError(dwErrors);
 port[com].Read(sBuf1, 100);
 for (UINT po=0;po<=strlen(Out);po++)
	{
	 port[com].Write (Out+po, 1);
	 //UL_INFO((LOGID,"[%d] O[%d] = 0x%x",device,po,(UCHAR)Out[po]));
	}
 for (UINT rr=0; rr<40; rr++)
	{
	 port[com].GetStatus(stat);
     if (stat.cbInQue)
		{
		 dwbr1 = port[com].Read(sBuf1, 200);
		 break;
		}
	 Sleep (100);
	}
 //if (dwbr1>0 && dwbr1<100) for (UINT i=0;i<dwbr1;i++) UL_INFO((LOGID,"[%d]  [%d] = 0x%x [%c]",device,i,(UCHAR)sBuf1[i],(UCHAR)sBuf1[i]));
}
//-------------------------------------------------------------------
UINT DestroyDriver()
{
  if (my_service)		
    {
      INT ecode = loServiceDestroy(my_service);
      UL_INFO((LOGID, "%!e loServiceDestroy(%p) = ", ecode));	// destroy derver
      DeleteCriticalSection(&lk_values);						// destroy CS
      my_service = 0;		
    }
 for (UINT i=0; i<Com_number; i++) port[i].Close();
 UL_INFO((LOGID, "Close COM-port"));						// write in log
 return	1;
}
//---------------------------------------------------------------------------------------------------
// ReadMConf (blok, blok, DeviceRU[devNum].name, DeviceRU[devNum].tag_num);
// ReadMConf (0, 0, "Instrumentalniy", 7);
VOID ReadMConf (INT dev, SHORT blok, CHAR* name, UINT tagn)
{
UINT max=1;
//if (DeviceRU[dev].type==3) max=1; else max=MAX_PIPE_NUMS;
for (UINT slot=1; slot<=max; slot++)
for (UINT r=1; r<=tagn; r++)
{
 DR[tag_num].nmodule = slot;
 DR[tag_num].kanal = r;
 DR[tag_num].device = dev;
 sprintf (DR[tag_num].module, name);
 DR[tag_num].status = 0;
 strcpy (DR[tag_num].value,"0.0");
 DR[tag_num].type = 0;
 printf ("[logika] Tag [%d] [Device %d.Module %d %s.Channel %d]\n", tag_num, dev, slot, DR[tag_num].module, r);
 UL_INFO ((LOGID,"Tag [%d] [Device %d.Module %d %s.Channel %d]", tag_num, dev, slot,DR[tag_num].module, r));  
 tag_num++;
}
}
//---------------------------------------------------------------------------------------------------
// (10 01) (10 1F) 1D (10 02) 09 30 09 33 0C (10 03) 74 D3 	
// (10 01) 01 80 (10 1F) 1D 53 30 (10 02) 09 30 09 39 39 0C (10 03) 61 81 	
CHAR* FormSequence (UCHAR lNum, UCHAR nFunc, UCHAR nChannal, UINT npAdr, UCHAR nPipe, SPrint Msp)
{
 SYSTEMTIME stb,ste;	 
 CHAR buf[250]; CHAR *pbuf=buf;
 CHAR buffer[13][50]; UCHAR len=7; SHORT startm=0;
 sprintf (buf,"i mozhesh ne otvechat");
 
 if (nFunc==0x1d) 
	{
	 sprintf (buf,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c%s%c%c%c",DLE,SOH,lNum,0x80,DLE,ISI,nFunc,0x53,0x30,DLE,STX,HT,nPipe+48,HT,_itoa(npAdr,buffer[0],10),FF,DLE,ETX);
	 //UL_INFO ((LOGID,"961[L%d][A%d](%d)",lNum,nPipe,npAdr));
//	 UL_INFO ((LOGID,"nPAdr=(%d)[%s] | nPipe=(%d)",npAdr,_itoa(npAdr,buffer[0],10),nPipe));
	}
 if (nFunc==0xc) 
	{
	 sprintf (buf,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c%s%c%c%c%c%c%c%c",DLE,SOH,lNum,0x80,DLE,ISI,nFunc,0x53,0x30,DLE,STX,HT,nPipe+48,HT,_itoa(npAdr,buffer[0],10),HT,0x30,HT,0x31,FF,DLE,ETX);
	 //ULOGW ("[A%d](%d)",nPipe,npAdr);
	}
 if (nFunc==0xe)  
	{	 	 
	 FileTimeToSystemTime(&Msp.start,&stb);
	 FileTimeToSystemTime(&Msp.end,&ste);
	 //UL_INFO ((LOGID,"961[L%d][A%d](%d)[%d.%d.%d %d:%d - %d.%d.%d %d:%d]",lNum,Msp.pipe,Msp.type,stb.wDay,stb.wMonth,stb.wYear,stb.wHour,stb.wMinute,ste.wDay,ste.wMonth,ste.wYear,ste.wHour,ste.wMinute));
	 sprintf (buf,"%c%c%c%c%c%c%c%c%c%c%c%c%c%c%s%c%c%s%c%s%c%s%c%s%c%s%c%s%c%c%s%c%s%c%s%c%s%c%s%c%s%c%c%c",DLE,SOH,lNum,0x80,DLE,ISI,nFunc,0x53,0x30,DLE,STX,HT,nPipe+48,HT,_itoa(npAdr,buffer[0],10),FF,
			  HT,_itoa(ste.wDay,buffer[1],10),HT,_itoa(ste.wMonth,buffer[2],10),HT,_itoa(ste.wYear,buffer[3],10),HT,_itoa(ste.wHour,buffer[4],10),HT,_itoa(ste.wMinute,buffer[5],10),HT,_itoa(ste.wSecond,buffer[6],10),FF,
			  HT,_itoa(stb.wDay,buffer[7],10),HT,_itoa(stb.wMonth,buffer[8],10),HT,_itoa(stb.wYear,buffer[9],10),HT,_itoa(stb.wHour,buffer[10],10),HT,_itoa(stb.wMinute,buffer[11],10),HT,_itoa(stb.wSecond,buffer[12],10),FF,
			  DLE,ETX);
	 //UL_INFO ((LOGID,"[%s]",buf));
	}
  if (nFunc==0x1d || nFunc==0xe || nFunc==0xc)
	{
	 //UL_INFO ((LOGID,"CRCode (%d)",strlen(buf)));
	 SHORT ks = CRCode ((UCHAR*)buf+2, strlen(buf)-2);
	 sprintf (buf,"%s%c%c",buf,(UCHAR)(ks>>8),(UCHAR)(ks%256));
	}											
 if (nFunc==0x48 || nFunc==0x59 || nFunc==0x4D) // Запрос поиска записи в часовом архиве:  0x10 NT 0x48 гг мм дд чч КС 0x16
	{											// Запрос поиска записи в суточном архиве: 0x10 NT 0x59 гг мм дд 0x00 КС 0x16
	 FileTimeToSystemTime(&Msp.start,&stb);		// Запрос поиска записи в месячном архиве: 0x10 NT 0x4D гг мм 0x00 0x00 КС 0x16
	 //if (nFunc==0x48) UL_INFO ((LOGID,"941[L%d][%d](%d)(%d)[%d.%d.%d %d:00]",lNum,nFunc,Msp.pipe,Msp.type,stb.wDay,stb.wMonth,stb.wYear,stb.wHour));
	 //if (nFunc==0x59) UL_INFO ((LOGID,"941[L%d][%d](%d)(%d)[%d.%d.%d 00:00]",lNum,nFunc,Msp.pipe,Msp.type,stb.wDay,stb.wMonth,stb.wYear));
	 //if (nFunc==0x4D) UL_INFO ((LOGID,"941[L%d][%d](%d)(%d)[01.%d.%d 00:00]",lNum,nFunc,Msp.pipe,Msp.type,stb.wMonth,stb.wYear));

	 if (nFunc==0x48) sprintf (buf,"%c%c%c%c%c%c%c",DLE,lNum,nFunc,stb.wYear-1900,stb.wMonth,stb.wDay,stb.wHour);
	 if (nFunc==0x59) sprintf (buf,"%c%c%c%c%c%c%c",DLE,lNum,nFunc,stb.wYear-1900,stb.wMonth,stb.wDay,ZERO);
	 if (nFunc==0x4D) sprintf (buf,"%c%c%c%c%c%c%c",DLE,lNum,nFunc,stb.wYear-1900,stb.wMonth,ZERO,ZERO);
	}
 if (nFunc==0x52) // Запрос чтения ОЗУ: 0x10 NT 0x52 А1 А0 КБ 0x00 КС 0x16
	{
	 sprintf (buf,"%c%c%c%c%c%c%c",DLE,lNum,nFunc,(UCHAR)((npAdr+0x200)%256),(UCHAR)((npAdr+0x200)/256),4,ZERO);
//	 UL_INFO ((LOGID,"[A%d](%d)",nPipe,npAdr+0x200));
	}
 if (nFunc==0x45) // Запрос чтения FLASH-памяти: 0x10 NT 0x45 N1 N0 K 0x00 КС 0x16
	{
	 //10 01 45 89 01 15 00 1A 16 
	 if (nPipe==88) sprintf (buf,"%c%c%c%c%c%c%c",DLE,lNum,nFunc,(UCHAR)(npAdr%256),(UCHAR)(npAdr/256),3,ZERO);
	 // (0x200 + nPadr*0x10)/0x40
	 else sprintf (buf,"%c%c%c%c%c%c%c",DLE,lNum,nFunc,(UCHAR)(8+npAdr/4),(UCHAR)(0),1,ZERO);
//	 UL_INFO ((LOGID,"[A%d](%d)",nPipe,npAdr));
	}
 if (nFunc==0x3f)
	{
	 sprintf (buf,"");
	 for (UINT i=0;i<16; i++) sprintf (buf,"%s%c",buf,0xff);
	 sprintf (buf,"%s%c%c%c%c%c%c%c",buf,DLE,lNum,nFunc,ZERO,ZERO,ZERO,ZERO);
	 len=7+16; startm = 16;
//	 UL_INFO ((LOGID,"[A%d](%d)",nPipe,npAdr));
	}
 if (nFunc==0x3f || nFunc==0x45 || nFunc==0x52 || nFunc==0x48 || nFunc==0x59 || nFunc==0x4D)
	{
	 SHORT ks = KScode ((UCHAR*)buf+1+startm, len-1-startm);
	 buf[len]=(UCHAR)(ks);
	 buf[len+1]=UK;
	 //sprintf (buf,"%s%c%c",buf,(UCHAR)(ks),UK);
	}
 return pbuf;
}
//---------------------------------------------------------------------------------------------------
VOID ReadLData (SHORT blok)
{
 CHAR *sBuf1; sBuf1 = new CHAR [170];
 port[blok].Read(sBuf1, 150);
 delete sBuf1;
}
//---------------------------------------------------------------------------------------------------
BOOL ReadCPT (SHORT adr, SHORT blok, SHORT type, SHORT pAdr)
{
 CHAR *sBuf2,*sBuf1,*Out;
 sBuf2 = new CHAR [400];	// buffer for rs-232
 sBuf1 = new CHAR [17000];
 Out = new CHAR[150];
 SPrint Msp={0}; UCHAR len = 0; 
 //UL_INFO((LOGID,"A(%d) B[%d] T[%d] p[%d]",adr,blok,type,pAdr));
 DWORD dwStoredFlags = EV_RXCHAR | EV_TXEMPTY | EV_RXFLAG;	
 //port[blok].Read(sBuf2, 200);
 port[blok].SetMask (dwStoredFlags);
 for (UINT i=0;i<30;i++) sBuf1[i]=0;
 if (type) 
	{
	 strcpy (Out,FormSequence((UCHAR)adr,0x1d,0,pAdr,0, Msp)); // 961
	 len=strlen(Out);
	}
 else
	{
	 /*sprintf (Out,"");
	 for (UINT i=0;i<16; i++) sprintf (Out,"%s%c",Out,0xff);
	 sprintf (Out,"%s%c%c%c%c%c%c%c",Out,DLE,(UCHAR)adr,0x45,(UCHAR)(8+pAdr/4),(UCHAR)(0),1,ZERO);
	 len=23; SHORT startm = 16;
	 SHORT ks = KScode ((UCHAR*)Out+1+startm, len-1-startm);
	 Out[len]=(UCHAR)(ks);
	 Out[len+1]=UK;	 
	 len=25;*/
	 if (pAdr)
	 	memcpy (Out,FormSequence((UCHAR)adr,0x45,0,pAdr,0,Msp),9);		// 941
	 else
	 	memcpy (Out,FormSequence((UCHAR)adr,0x3f,0,pAdr,0,Msp),25);		// 941
	 if ((UCHAR)Out[0]==(UCHAR)0xff) len=25; else len=9;
	}
 port[blok].Purge(PURGE_RXABORT|PURGE_RXCLEAR|PURGE_TXABORT|PURGE_TXCLEAR);
 for (UINT po=0;po<len;po++)
	{	
	 port[blok].Write (Out+po, 1);
	 #ifdef LOGS
	 UL_INFO((LOGID,"o(%d)[%d] = 0x%x",pAdr,po,(UCHAR)Out[po]));
	 #endif
//	 port[blok].WaitEvent (dwStoredFlags);
	}
 //-----------------------------------------------
 if (type) Sleep(1000);	// !!!!!!!!!!!!!!!!!!!!
 else Sleep(2200);
 DWORD dwBytesRead=-1;
 DWORD dwbr1=0;
 BOOL bSuccess=FALSE;
 for (UINT rr=0; rr<20; rr++)
	{
	 COMSTAT comstat;
	 port[blok].GetStatus(comstat);
	 if (comstat.cbInQue)
		{
		 dwBytesRead = port[blok].Read(sBuf2, 100);
	 	 for (UINT i=0;i< dwBytesRead;i++) sBuf1[dwbr1+i]=sBuf2[i];
 		 dwbr1=dwbr1+dwBytesRead;
		 //UL_INFO ((LOGID,"[%d/%d] dwBytesRead = [%d/%d]",adr,blok,dwBytesRead,dwbr1));
		}
	 Sleep (50);
	}
 //ULOGW("dwBytesRead = %d / GetLastError() = %d",dwBytesRead,GetLastError());
//-----------------------------------------------
 #ifdef LOGS
  for (i=0;i<dwbr1;i++) { sBuf1[i]=(UCHAR)sBuf1[i]; UL_INFO((LOGID,"(%d)[%d] = 0x%x(%c)",pAdr,i,(UCHAR)sBuf1[i],(UCHAR)sBuf1[i])); }
 #endif
 BOOL ok=FALSE;
 if (type) 
	{
	 if (AnalyseSequence ((CHAR*)sBuf1,dwbr1,blok) && alog[blok].npAdr==pAdr) ok=TRUE;
	 else { ReadLData (blok); ok=FALSE; }
	}
 else 
	{
	 if (AnalyseAnswer ((CHAR*)sBuf1,dwbr1,blok,pAdr)) ok=TRUE;
	 else ok=FALSE;
	}
 delete sBuf2; delete sBuf1; delete Out;
 return ok;
}
//---------------------------------------------------------------------------------------------------
BOOL ReadAPC79 (SHORT adr, SHORT blok)
{
 DWORD dwbr1=0;
 CHAR sBuf2 [400];	// buffer for rs-232
 CHAR sBuf1 [17000];
 UL_INFO ((LOGID,"ReadAdrAPC79 %d",adr));
 UINT len3=strlen((CHAR*)sBuf3);
 UINT len52=strlen((CHAR*)sBuf52);
 UINT len99=strlen((CHAR*)sBuf99);
 SHORT res = CRCode (sBuf52+2,len52-2); sBuf52[len52]=(UCHAR)(res>>8); sBuf52[len52+1]=(UCHAR)(res%256);
 res = CRCode (sBuf3+2,len3-2); sBuf3[len3]=(UCHAR)(res>>8); sBuf3[len3+1]=(UCHAR)(res%256);
 res = CRCode (sBuf99+2,len99-2); sBuf99[len99]=(UCHAR)(res>>8); sBuf99[len99+1]=(UCHAR)(res%256);
 for (UINT nump=0;nump<3;nump++)
	{
	 DWORD dwStoredFlags = EV_RXCHAR | EV_TXEMPTY | EV_RXFLAG;	
	 port[blok].SetMask (dwStoredFlags);
	 if (adr==3)  
		{
		 UINT i=0;
		 #ifdef LOGS
			for (i=0;i<strlen((CHAR*)sBuf3);i++) UL_INFO((LOGID,"[%d] = 0x%x",i,(UCHAR)sBuf3[i]));
		 #endif
		 for (i=0;i<sizeof(sBuf3)-1;i++) 
			{
			 port[blok].Write (sBuf3+i,1);
			 //port[blok].WaitEvent (dwStoredFlags);
			}
		}
	 if (adr==99) 
		{
		 for (UINT i=0;i<sizeof(sBuf99)-1;i++) 
			{
			 port[blok].Write (sBuf99+i,1);
			 port[blok].WaitEvent (dwStoredFlags);
			}
		}
	 if (adr==52) 
		{
		 for (UINT i=0;i<sizeof(sBuf52)-1;i++) 
			{
			 port[blok].Write (sBuf52+i,1);
			 port[blok].WaitEvent (dwStoredFlags);
			}
		}
	 Sleep (100);
	 DWORD dwBytesRead = port[blok].Read(sBuf1, 1000);
	 dwbr1 = dwBytesRead;
	 while (dwBytesRead)
		{
		 dwBytesRead = port[blok].Read(sBuf2, 200);
		 for (UINT i=0;i< dwBytesRead;i++) sBuf1[dwbr1+i]=sBuf2[i];
		 dwbr1 = dwbr1+dwBytesRead;
		}
	 sBuf1[dwbr1] = 0;
	 UL_INFO ((LOGID,"ReadFile %d",dwbr1));
	 if (dwbr1) nump=10;
	}
 #ifdef LOGS
 for (UINT i=0;i<dwbr1;i++) UL_INFO ((LOGID,"[%d] = 0x%x",i,(UCHAR)sBuf1[i]));
 #endif
 if (dwbr1!=0)
	{
	 if (AnalyseSequence ((CHAR*)sBuf1,dwbr1,blok))
		{
		 //UL_INFO ((LOGID,"[%d](%x->%x) %x %s|[%d](%d) (%d-%d) [%s %s]",alog[blok].checksym,alog[blok].from,alog[blok].to,alog[blok].func,alog[blok].head,alog[blok].pipe,alog[blok].npAdr,alog[blok].from_param,alog[blok].quant_param,alog[blok].data[0],alog[blok].type[0]));
		 if (alog[blok].checksym)
			{
			 if (adr==3) strcpy(apc[blok].spec,alog[blok].data[0]);		// Спецификация внешнего оборудования. peslraahhv.
			 if (adr==99) strcpy(apc[blok].version,alog[blok].data[0]);	// Версия адаптера.
			 if (adr==52) apc[blok].period = atoi (alog[blok].data[0]);	// Периодичность опроса
			 return TRUE;
			}
		}
	} 
 return FALSE;
}
//----------------------------------------------------------------------------------------
BOOL AnalyseAnswer (CHAR* dat,UINT len, UINT blok, SHORT pAdr)
{
 alog[blok].checksym = FALSE;
 alog[blok].quant_param = 0;
 
 UINT i=0,startm=0, cntNS=0; BOOL start=FALSE;
 while (i<len)
	{
	 if (dat[i]==DLE && (UCHAR)dat[i+1]<12 && (UCHAR)dat[i+2]>0x3e && !start)
		{
		 alog[blok].from = (UCHAR)dat[i+1];		 
		 alog[blok].func=(UCHAR)dat[i+2];
		 startm=i+1; i=i+3; start=TRUE;
		}
	 if (dat[i]==DLE && dat[i+2]==0x21 && dat[i+3]==0x3)
		{
		 //UL_DEBUG ((LOGID,"No such data"));
		 return TRUE;
		}
	 if ((dat[i]==UK && dat[i+1]!=UK) && ((pAdr<0x189) || ((pAdr>=0x189) && (i-2-startm>62))))
	 if ((!(len>60 && i<58)) && (!(len>6 && len<10 && i<7)))
	 //if (((dat[i]==UK) && (pAdr<0x189)) || ((dat[i]==UK) && (pAdr>=0x189) && (i-2-startm>62)))
		{
 		 SHORT ks=KScode ((UCHAR*)dat+startm,i-startm-1);
		 //UL_DEBUG ((LOGID,"KS [%d->%d] %x (%x) [%d]",startm,i-1-1,ks,(UCHAR)dat[i-1],len));
		 if (((UCHAR)ks==(UCHAR)dat[i-1]) || (i==67 && len==68)) 
			{
			 alog[blok].checksym = TRUE;
		 	 if (alog[blok].func==0x3f) // Ответ: 0x10 NT 0x3F 0x92 0x29 VX КС 0x16
				{
				 memcpy (alog[blok].data[0],(dat+startm+2),3);
				 //UL_DEBUG ((LOGID,"[%x][%x][%x]",alog[blok].data[0][0],alog[blok].data[0][1],alog[blok].data[0][2]));
				 alog[blok].data[0][3]=0;
				 strcpy (alog[blok].type[0],"");
				 strcpy (alog[blok].time[0],"");
				 alog[blok].quant_param = 1;
				}
			 if (alog[blok].func==0x45)	// Ответ: 0x10 NT 0x45 страница (N) КС 0x16
				{
				 if (pAdr<0x189) // Flash (param)
					{
					 memcpy (alog[blok].data[0],(dat+startm+4+2+(pAdr%4)*16),8);
					 alog[blok].data[0][8]=0;
					 strcpy (alog[blok].type[0],"");
					 strcpy (alog[blok].time[0],"");
					 alog[blok].quant_param = 1;
					}
				 if (pAdr>=0x189)	// Flash (NS)
					{					 
					 memcpy (alog[blok].data[cntNS],(dat+startm+2),64); // + 4?? check this
					 alog[blok].data[cntNS][64]=0;
					 alog[blok].quant_param = cntNS;
					 cntNS++;
					 //UL_DEBUG ((LOGID,"cntNS = [%d]",cntNS));
					}
				}
			 if (alog[blok].func==0x52)	// Ответ: 0x10 NT 0x52 Дамп ОЗУ КС 0x16
				{		 
				 memcpy (alog[blok].data[0],(dat+startm+2),4);
				 alog[blok].data[0][8]=0;
				}
			 if (alog[blok].func==0x48 || alog[blok].func==0x59 || alog[blok].func==0x4d)
			 if (len==69 || len==68)
				{  //0x10 NT 0x48 блок данных 64 байта КС 0x16
				 /*if (len==68)
					 for (UINT j=1; j<10; j++)
						{
						 if (*(dat+startm+2+j*4+2)>0x76 && *(dat+startm+2+j*4+2)<0x88)
						 if (*(dat+startm+2+(j+1)*4+2)>0x76 && *(dat+startm+2+(j+1)*4+2)<0x88)
						 if (*(dat+startm+2+(j+2)*4+2)>0x76 && *(dat+startm+2+(j+2)*4+2)<0x88)
							{
							 //for (UINT r=68; r>(startm+2+j*4); r--)
							//	 *(dat+startm+2+r)=*(dat+startm+1+r);
							 UL_DEBUG ((LOGID,"redakt"));
							 break;
							}
						}*/

				 for (UINT j=0; j<12; j++)
					{
					 memcpy (alog[blok].data[j],(dat+startm+2+j*4),4);
					 if ((UCHAR)alog[blok].data[j][3]<0x76 || (UCHAR)alog[blok].data[j][3]>0x8a) 
						{
						 alog[blok].data[j][0]=0; alog[blok].data[j][1]=0; alog[blok].data[j][2]=0; alog[blok].data[j][3]=0; alog[blok].data[j][4]=0;
						}
					 //UL_DEBUG ((LOGID,"[%d](%d) %x %x %x %x",alog[blok].func,j,(UCHAR)alog[blok].data[j][0],(UCHAR)alog[blok].data[j][1],(UCHAR)alog[blok].data[j][2],(UCHAR)alog[blok].data[j][3]));
					 alog[blok].data[j][5]=0;
					}
				 alog[blok].quant_param = 11;
				}
			}
		 else 
			{
			 alog[blok].checksym = FALSE;
			 for (UINT j=0;j<len;j++) UL_INFO((LOGID,"[%d] [%d] = 0x%x [%c]",pAdr,j,(UCHAR)dat[j],(UCHAR)dat[j]));
			 UL_DEBUG ((LOGID,"wrong checksum alog[blok].checksym=%d(rec=%x,must=%x) at pos=%d",alog[blok].checksym,ks,(UCHAR)dat[i-1],i));
			}		 
		 if (pAdr<0x189) return TRUE;
		}
	 i++;
	}
 if (pAdr>=0x189) return TRUE;
 else  return FALSE;
}
//----------------------------------------------------------------------------------------
BOOL AnalyseSequence (CHAR* dats,UINT len, UINT blok)
{
 CHAR buffer[150]; UINT quantZp=0; CHAR dat[1000];
 UINT i=0,start=0, startm=0;
 if (len>1000) len=990;
 dats[len]=0; strcpy (dat,dats);

 //UL_INFO ((LOGID,"AnalyseSequence %d (%d)",blok,len));
 dat[len]=0; dat[len+1]=0x9;
 alog[blok].checksym = FALSE;

 while (i<len && i<400)
	{
	 //ULOGW ("<F1 dat[%d]=%d",i,dat[i]);
	 if (dat[i]==DLE && dat[i+1]==SOH) 
		{		 
		 if (dat[i+2]!=DLE)
			{
			 alog[blok].from = (UCHAR)dat[i+3];
			 alog[blok].to = (UCHAR)dat[i+2];			 
			 startm=i+2; i=i+4;
			}
		 else 
			{
			 i=i+2; startm=i;
			}
		 //ULOGW ("dat[%d]=%x = dat[%d]=%x len=%d",i,dat[i],i+1,dat[i+1],len);
		 while (i<len && i<400)
			{
			 //ULOGW ("<F3 dat[%d]=%d",i,dat[i]);
			 if (dat[i]==DLE && dat[i+1]==STX) break;
			 if (dat[i]==DLE && dat[i+1]==ISI)
				{ 
				 alog[blok].func=(UCHAR)dat[i+2];
				 start=i+3; // DataHead
				 //ULOGW("[ISI] = 0x%x 0x%x 0x%x",dat[i],dat[i+1],dat[i+2]);
				 //ULOGW("alog.func = 0x%x",dat[i+2]);
				}			 
			 i++;
			}		  
		}
//	 ULOGW ("<F11 dat[%d]=%d",i,dat[i]);
	 if (dat[i]==DLE && dat[i+1]==STX)
		{
		 //UL_INFO ((LOGID,"[%d] %d %d [%d]",blok,start,i-start,alog[blok].func));
		 if (i-start>100) UL_INFO ((LOGID,"!!!! head"));
		 strncpy (alog[blok].head,dat+start,i-start);		 
		 i=i+2;
		 //ULOGW ("F3 dat[%d]=%d",i,dat[i]);
		 if (dat[i]==HT) 
			{
			 alog[blok].pipe = dat[i+1]-48; i=i+2;
			 //ULOGW ("alog.pipe=%d",alog.pipe);
			 if (dat[i]==HT)
				{
				 //ULOGW ("F5 dat[%d]=%d",i,dat[i]);
				 i++; start=i;
				 while (dat[i]!=FF && dat[i]!=HT && i<len) i++; 
				 //UL_INFO ((LOGID,"[%d]  [%d] %d -> %d",blok,i,dat+start,i-start));

				 if (i-start<100) strncpy (buffer,dat+start,i-start); else UL_INFO ((LOGID,"!!!! buffer"));
				 buffer[i-start]=0;
				 //UL_INFO ((LOGID,"buffer=%s",buffer));
				 alog[blok].npAdr = atoi (buffer);
				 //UL_INFO ((LOGID,"[%d] alog[blok].to [0x%x] alog[blok].from [0x%x] pipe=%d npAdr = %d func = 0x%x",blok,alog[blok].to,alog[blok].from,alog[blok].pipe,alog[blok].npAdr,alog[blok].func));
				 
				 if (!alog[blok].npAdr) return FALSE;
				 alog[blok].from_param = 0;
				 alog[blok].quant_param = 0;
				 if (alog[blok].func==0x14)
					{
					 i++; start=i;
					 while (dat[i]!=FF && dat[i]!=HT && i<len) i++;
					 if (i-start>100) UL_INFO ((LOGID,"!!!! buffer(0x14)"));
					 strncpy (buffer,dat+start,i-start);					 
					 alog[blok].from_param = atoi (buffer);
					 i++; start=i;
					 while (dat[i]!=FF && dat[i]!=HT && i<len) i++;
					 if (i-start>100) UL_INFO ((LOGID,"!!!! buffer(0x14)"));
					 strncpy (buffer,dat+start,i-start);
					 alog[blok].quant_param = atoi (buffer);
					}
				 if (alog[blok].func==0x7f) return TRUE;
				 if (alog[blok].func==0x16) 
					{
 					 i++; start=i;
					 while (dat[i]!=FF && i<len) i++;
					 i++;
					 while (dat[i]!=FF && i<len) i++;
					 //UL_INFO ((LOGID,"start from = %d",i));
					}				 
	 			 UINT pos=0;
				 CHAR datt[5000];
				 CHAR value[ARCHIVE_NUM_MAX][30];
				 if (len>i+11)
					{
					 if (len-i-7>1000) UL_INFO ((LOGID,"!!!! datt=%d",len-i-7));
					 strncpy (datt,dat+i+2,len-i-7);
					 for (UINT o=0;o<=len-i-7;o++)
						{
						 if (*(dat+i+2+o)==0xc && *(dat+i+3+o)==0x10 && *(dat+i+4+o)==0x3) { datt[o]=0x9; datt[o+1]=0; o=len-i-6;}
						 else datt[o]=(UCHAR)*(dat+i+2+o);
						 #ifdef LOGS
						 UL_DEBUG ((LOGID,"datt[%d]=%x",o,datt[o]));
						 #endif
						}
					 datt[o]=0;
					 CHAR* token = strtok(datt,"\t");
					 while(token!=NULL)
						{
						 if (strlen(token)<28) sprintf (value[pos],token);
						 else sprintf (value[pos]," ");

						 //UL_INFO ((LOGID,"value[%d]=%s",pos,value[pos]));
						 token = strtok(NULL,"\t"); pos++;
						}
					}
				 else { value[0][0]=0; value[1][0]=0; value[2][0]=0; }
				 if (alog[blok].func==0x16)
					{
					 //UL_INFO ((LOGID,"log[blok].func==0x16"));
					 // HT Значение HT Единицы HT Метка времени
					 // HT Значение HT HT Метка времени
					 BOOL some_ed=FALSE;
					 BOOL no_ed=FALSE;
					 //UL_INFO ((LOGID,"%s %s %s %s %s",value[0],value[1],value[2],value[3],value[4]));

					 if (strlen(value[4])>14 && strlen(value[2])>14)
						{ 
						 some_ed=TRUE;
						 //UL_INFO ((LOGID,"%d=%d",strlen(value[4]),strlen(value[6])));
						}
					 if (strlen(value[1])>14 && alog[blok].npAdr>100) no_ed=TRUE;
					 for (UINT j=0,r=0;j<pos;j++)
						{
						 if (pos<2 || (!strlen(value[j]) && !strlen(value[j+1]))) continue;
						 if (strlen(value[j])>80) UL_INFO ((LOGID,"!!!! value"));
						 strcpy (alog[blok].data[r],value[j]); 
						 j++;
						 if (!no_ed)
							{
							 if (some_ed) strcpy (alog[blok].type[r],value[1]);
							 if (!some_ed) { strcpy (alog[blok].type[r],value[j]);	j++;}
							 if (j==1 && some_ed) j++;
							}
						 else strcpy (alog[blok].type[r],"-");
						 if (strlen(value[j])>27) 
							{
							 strcpy (alog[blok].time[r],"");
							 UL_INFO ((LOGID,"!!!! value-t"));
							}
						 else strcpy (alog[blok].time[r],value[j]);

						 UINT ln = strlen(alog[blok].time[r]);
						 alog[blok].time[r][ln-1]=0;
						 if (strlen(alog[blok].time[r])<10) break;
						 if (strlen(alog[blok].type[r])>10) break;
						 //UL_INFO ((LOGID,"[%s] (%s) %s",alog[blok].time[r],alog[blok].type[r],alog[blok].data[r]));
						 r++; quantZp++;
						}
					 alog[blok].quant_param = quantZp;
					 //UL_INFO ((LOGID,"quantZp==%d",quantZp));
					}
				if (alog[blok].func==0x14 || alog[blok].func==0x3)
					{
					 strncpy (alog[blok].data[0],value[0],17); alog[blok].data[0][17]=0;
					 strncpy (alog[blok].type[0],value[1],17); alog[blok].type[0][17]=0;
					 strcpy (alog[blok].time[0],"");
					 //if (alog[blok].npAdr>100) sprintf (alog[blok].data[0],"%f",(DOUBLE)((DOUBLE)rand()/(DOUBLE)10737));
					}
				}
			 else { UL_DEBUG ((LOGID,"HT1 %x %x %x",dat[i-1],dat[i],dat[i+1])); return FALSE; }
			}
		 else { UL_DEBUG ((LOGID,"HT2 %x %x %x",dat[i-1],dat[i],dat[i+1])); return FALSE; }
		}
	 if (dat[i]==DLE && dat[i+1]==ETX)
		{
		 //UL_DEBUG ((LOGID,"[%d] KS [%d->%d]",blok,startm,len-startm-2));
		 SHORT ks=CRCode ((UCHAR*)dat+startm,len-startm-2);
		 //UL_DEBUG ((LOGID,"[%d] KS [%d->%d] %x",blok,startm,len-startm-2,ks));
		 if (((UCHAR)(ks/256)==(UCHAR)dat[i+2])&&((UCHAR)(ks%256)==(UCHAR)dat[i+3])) alog[blok].checksym = TRUE;
		 else alog[blok].checksym = TRUE;
		 //else alog.checksym = FALSE;

		 //UL_DEBUG ((LOGID,"([%x %x] [%x %x])",(UCHAR)(ks>>8),(UCHAR)ks%256,(UCHAR)dat[i+2],(UCHAR)dat[i+3]));
		 //break;
		 //UL_DEBUG ((LOGID,"alog[blok].checksym=%d",alog.checksym));
		 return TRUE;
		}
	 i++;
	}
 return TRUE;
}
//---------------------------------------------------------------------------------------------------
// Функция вычисляет и возвращает циклический код для последовательности из len байтов, указанной *msg.
// Используется порождающий полином: (X в степени 16)+(X в степени 12)+(X в степени 5)+1.
// Полиному соответствует битовая маска 0x1021.
SHORT CRCode (BYTE *msg, INT len)
{
SHORT crc,j;
crc = 0;
while (len-- > 0)
{
crc = crc ^ (INT) *msg++ << 8;
for (j=0;j<8;j++)
{
if(crc&0x8000) crc = (crc << 1) ^ 0x1021;
else crc <<= 1;
}
}
return crc;
}
//---------------------------------------------------------------------------------------------------
SHORT KScode (BYTE *msg, INT len)
{
SHORT crc;
crc = 0;
while (len-- > 0) 
	{
	 //UL_DEBUG ((LOGID,"[0x%x]",(INT) *msg));
	 crc = crc + (INT) *msg++;
	 //UL_DEBUG ((LOGID,"0x%x",crc));
	}
crc=crc^0xff;
return crc;
}
//-----------------------------------------------------------------------------------
CHAR* ReadParam (CHAR *SectionName,CHAR *Value)
{
	CHAR buf[150], string1[50], string2[50]; CHAR ret[150]={0};
CHAR *pbuf=buf; CHAR *bret=ret;
UINT s_ok=0;
sprintf(string1,"[%s]",SectionName);
sprintf(string2,"%s=",Value);
rewind (CfgFile);
while(!feof(CfgFile))
 if(fgets(buf,50,CfgFile)!=NULL)
	if (strstr(buf,string1))
		{ s_ok=1; break; }
if (s_ok)
	{
	 while(!feof(CfgFile))
		{
		 if(fgets(buf,100,CfgFile)!=NULL)
			{
			 if (strstr(buf,"[")==NULL && strstr(buf,"]")==NULL)
				{
				 for (s_ok=0;s_ok<strlen(buf)-1;s_ok++) if (buf[s_ok]==';') buf[s_ok+1]='\0';				 
				 if (strstr(buf,string2))
					{
					 for (s_ok=0;s_ok<strlen(buf)-1;s_ok++)
						if (s_ok>=strlen(string2)) buf[s_ok-strlen(Value)-1]=buf[s_ok];
							 buf[s_ok-strlen(string2)]='\0';					
					 strcpy(ret,buf);
					 return bret;
					}
				}
			  else { buf[0]=0; buf[1]=0; strcpy(ret,buf); return bret; }
			 }
		}	 	
 	 if (SectionName=="Port")	{ buf[0]='1'; buf[1]=0;}
	 buf[0]=0; buf[1]=0;
	 strcpy(ret,buf); return bret;
	}
else{
	 sprintf(buf, "error");			// if something go wrong return error
	 strcpy(ret,buf); return bret;
	}	
}
//------------------------------------------------------------------------------------
INT init_tags(VOID)
{
  UL_INFO((LOGID, "init_tags(%d)",tag_num));
  FILETIME ft;		//  64-bit value representing the number of 100-ns intervals since January 1,1601
  UINT rights=0;	// tag type (read/write)
  UINT ecode;
  BOOL arch=FALSE;
  GetSystemTimeAsFileTime(&ft);	// retrieves the current system date and time
  EnterCriticalSection(&lk_values);
  if (!tag_num) return 0;
  for (UINT i=0; i<tag_num; i++)
	{
	 //UL_ERROR((LOGID, "tags (%d)",DeviceRU[DR[i].device].tag_num));
	 rights=0;
	 rights = rights | OPC_READABLE;
	 UINT max = (UINT)DeviceRU[DR[i].device].tag_num;
	 for (UINT r=1; r<=max; r++)
		{
		 //UL_ERROR((LOGID, "device=%d (%d/%d) type=%d",DR[i].device,r,DeviceRU[DR[i].device].tag_num,DeviceRU[DR[i].device].type));
		 tn[i] = new char[DATALEN_MAX];	// reserve memory for massive
		 sprintf(tn[i],"unknown tag %d",i);
		 //UL_ERROR((LOGID, "{%d} %s %d %d d(%s)",DeviceRU[DR[i].device].type,DeviceRU[DR[i].device].name,DeviceRU[DR[i].device].com,DeviceRU[DR[i].device].device,Data762[r-1].name));
		 //com2-01/detal-k3-vz/Объемный расход воздуха/1-4 = 1.23 (1)
		 //com2-01/detal-k3-vz/Архив/ Архив часовой значений температуры газа/1-4 = 1.23 (1)
		 //192.1.95.88/ohrana-c1-vztp/P1tp/1-8 = 400.91 (1)			 
		 switch (DeviceRU[DR[i].device].type)
			{
			 case 11: if (!Archive961_1[r-1].type)
						{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive961_1[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive961_1[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;
			 case 12: if (!Archive961_2[r-1].type)
						{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive961_2[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive961_2[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;			 
			 case 13: if (!Archive961_4[r-1].type)
			 			{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive961_4[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive961_4[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;
			 case 15: if (!Archive761[r-1].type)
						{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive761[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive761[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;
			 case 16: if (!Archive762_6[r-1].type)
						{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive762_6[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive762_6[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;
			 case 17: if (!Archive762_7[r-1].type)
						{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive762_7[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive762_7[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;
			 case 21: if (!Archive941_1[r-1].type)
						{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive941_1[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive941_1[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;
			 case 22: if (!Archive941_2[r-1].type)
						{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive941_2[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive941_2[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;
			 case 24: if (!Archive941_4[r-1].type)
						{ sprintf(tn[i],"com%d-%0.2d/%s/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive941_4[r-1].name, DeviceRU[DR[i].device].pipe); arch=FALSE;}
					  else 
						{ sprintf(tn[i],"com%d-%0.2d/%s/Архив/%s (%d)",com_num[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].device,DeviceRU[DR[i].device].name, Archive941_4[r-1].name, DeviceRU[DR[i].device].pipe); arch=TRUE;}
					  break;
			}
		 if (DeviceRU[DR[i].device].type==3)
			{
			 //UL_ERROR((LOGID, "%s %s %s",ip_contr[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].name,WincName[i]));
			 //sprintf(tn[i],"%s/%s/%s [%d]",ip_contr[DeviceRU[DR[i].device].com],DeviceRU[DR[i].device].name, WincName[i],r-1); arch=FALSE;
			}			
		 VariantInit(&tv[i].tvValue);
		 WCHAR buf[DATALEN_MAX];
		 LCID lcid = MAKELCID(0x0409, SORT_DEFAULT); // This macro creates a locale identifier from a language identifier. Specifies how dates, times, and currencies are formatted
	 	 MultiByteToWideChar(CP_ACP, 0,tn[i], strlen(tn[i])+1,	buf, sizeof(buf)/sizeof(buf[0])); // function maps a character string to a wide-character (Unicode) string
		 if (!arch)
			{
			 V_R4(&tv[i].tvValue) = 0.0;
			 DR[i].type = 0;		// instant
			 V_VT(&tv[i].tvValue) = VT_R4;
			}
		 else
			{
			 V_BSTR(&tv[i].tvValue) = SysAllocString(L"");
			 DR[i].type = 1;		// archive
			 V_VT(&tv[i].tvValue) = VT_BSTR;
			}
		 //UL_ERROR((LOGID, "DR[i].type = %d",DR[i].type));
		 ecode = loAddRealTag_aW(my_service, &ti[i], (loRealTag)(i+1), buf, 0, rights, &tv[i].tvValue, 0, 0);
		 tv[i].tvTi = ti[i];
		 tv[i].tvState.tsTime = ft;
		 tv[i].tvState.tsError = S_OK;
		 tv[i].tvState.tsQuality = OPC_QUALITY_NOT_CONNECTED;
		 UL_TRACE((LOGID, "%!e loAddRealTag(%s) = %u (t=%d)", ecode, tn[i], ti[i], DR[i].type));
		 i++;
		}
	  i--;
	 } 
  LeaveCriticalSection(&lk_values);
  for (i=0; i<tag_num; i++) delete tn[i];
  if(ecode) 
  {
    UL_ERROR((LOGID, "%!e driver_init()=", ecode));
    return -1;
  }
  return 0;
}
//-------------------------------------------------------------------------------------------------
/*BOOL eConnection (INT _s_port, SOCKET &sck, CHAR* strC, UINT sock)
{
 INT rc; // Инициализация и проверка версии Windows Sockets
 rc = WSAStartup(MAKEWORD(2, 2), &WSAData);
 if(rc != 0)
	{
	 UL_ERROR ((LOGID,"WSAStartup failed. Error: %x",WSAGetLastError ()));
	 return FALSE;
	}
 sck = socket(AF_INET, SOCK_STREAM, 0);  // Создаем сокет 
 if(sck == INVALID_SOCKET)
	{
	 UL_ERROR ((LOGID,"Allocating socket failed. Error: %x",WSAGetLastError ()));
	 return FALSE;
	}
 // Устанавливаем адрес IP и номер порта
 srv_address.sin_family = AF_INET;
 srv_address.sin_addr.s_addr = INADDR_ANY;
 srv_address.sin_port = htons(sock);  
 //Связываем адрес IP с сокетом
 if(bind(sck, (LPSOCKADDR)&srv_address,sizeof(srv_address)) == SOCKET_ERROR)
	{	 
	 UL_ERROR ((LOGID,"Bind Error (%d)",WSAGetLastError ()));
	 closesocket(sck);		// При ошибке закрываем сокет	 
	 return FALSE;
	}
 // Устанавливаем адрес IP и номер порта
 // srv_address.sin_family = AF_INET;
 UL_INFO ((LOGID,"gethostbyname(%s)",strC));
 phe = gethostbyname(strC);
 if(phe == NULL) UL_ERROR ((LOGID,"gethostbyname %s not found | reason is %x",strC, WSAGetLastError ()));
 if(phe != NULL) // Копируем адрес узла
	{
	 memcpy((CHAR FAR *)&(srv_address.sin_addr), phe->h_addr, phe->h_length);
	 UL_INFO ((LOGID,"hostbyname \"%s\" is found ", strC));
	}
 else srv_address.sin_addr.s_addr = inet_addr(strC);
 if(connect(sck, (PSOCKADDR)&srv_address, sizeof(srv_address)) < 0)
	{
	 closesocket(sck);
	 UL_ERROR ((LOGID,"Establish a connection to %s ... connect Error | reason is %d",strC,WSAGetLastError ()));
	 return FALSE;
	}
 else 
	 UL_INFO ((LOGID,"Establish a connection to %s ... connect success",strC));
 BOOL l = TRUE;
 if (!ioctlsocket (sck,FIONBIO,(unsigned long* ) &l)) UL_INFO ((LOGID,"Switch to non-blocking mode"));
 return TRUE;
}*/
//--------------------------------------------------------------------------------------------
BOOL CreateRegKeys (DWORD dwD)
{
HKEY hk;
DWORD dwData=dwD; // "1" - start/ "0" - stop
if (RegCreateKey(HKEY_LOCAL_MACHINE,"SOFTWARE\\ASKUER OPC Server\\Server Run", &hk)) 
	{
	 UL_ERROR((LOGID, "Could not create registry entry"));
	 return FALSE;
	}
if (RegSetValueEx(hk,"ASKUER OPC stop Ack.",0,REG_DWORD,(LPBYTE) &dwData,sizeof(DWORD)))
	{
	 UL_ERROR((LOGID, "Could not create registry entry"));
	 return FALSE;
	}
UL_INFO((LOGID, "Create/update registry key \\ASKUER OPC stop Ack. = %d",dwData));
return TRUE;
}
//---------------------------------------------------------------------------------
UINT ReadRegKeys ()
{
HKEY hKey;
LONG lRet;
DWORD dwData;
DWORD dwBufLen = sizeof(DWORD);
if(RegOpenKeyEx(HKEY_LOCAL_MACHINE,TEXT("SOFTWARE\\ASKUER OPC Server\\Server Run"),0,KEY_QUERY_VALUE,&hKey) != ERROR_SUCCESS) 
{	UL_ERROR((LOGID, "Could not create registry entry"));
	return -1;}
lRet = RegQueryValueEx(hKey,TEXT("ASKUER OPC stop"),NULL,NULL,(LPBYTE) &dwData,&dwBufLen);
//UL_INFO((LOGID, "RegQueryValueEx %d",dwData));
if(lRet != ERROR_SUCCESS) return -1;
if (dwData==0) return 0;
else return 1;
}
//-------------------------------------------------------------------------------------------------
VOID CheckRegStatus ()
{ 
 if (ReadRegKeys()==1) // we must temporary stop server
	{ 
	  if (WorkEnable)	// but we dont stopped yet
		{
		  UL_INFO((LOGID, "Attempt stop server"));
		  //for (UINT device=0;device<devNum;device++) if (DeviceRU[device].type==3)
			//  closesocket (srv_socket[DeviceRU[device].com]);
		  for (UINT i=0; i<Com_number; i++) port[i].Close();
		  WorkEnable=FALSE;		// stop server
		  Sleep (500);			// pause to answer
		  CreateRegKeys (1);	// answer
		  UL_INFO((LOGID, "Server stopped"));
		}
	}
 else
	{
	  if (!WorkEnable)	// but we dont run yet
		{
		 UL_INFO((LOGID, "Attempt run server"));
		 for (UINT j=0;j<Com_number;j++)
			{
			 if (com_num[j] && speed[j])
				{
				 INT res=sio_checkalive (com_num[j], 3000);
				 switch (res)
					{
					 case D_ALIVE_CLOSE: UL_INFO((LOGID, "This port is alive and is not used by any program")); break;
					 case D_ALIVE_OPEN: UL_INFO((LOGID, "This port is alive and is used by some program")); break;
					 case D_NOT_ALIVE: UL_INFO((LOGID, "This port is not alive")); break;
					 case D_NOT_NPORT_COM: UL_INFO((LOGID, "The COM port number is not belonged to any configured NPort Server on the host")); break;
					 case D_SOCK_INIT_FAIL:	UL_INFO((LOGID, "Initialize the WinSock fail")); break;
					}
				 UL_INFO((LOGID, "Opening port COM%d on speed %d with parity %d and databits %d",com_num[j],speed[j], parity, databits));
				 if (parity==0) if (!port[j].Open(com_num[j],speed[j], SerialPort::EvenParity, databits, SerialPort::OneStopBit, SerialPort::NoFlowControl, FALSE)) { UL_ERROR((LOGID, "Error open COM-port")); return;}
				 if (parity==2) if (!port[j].Open(com_num[j],speed[j], SerialPort::NoParity, databits, SerialPort::OneStopBit, SerialPort::NoFlowControl, FALSE)) { UL_ERROR((LOGID, "Error open COM-port")); return;}
				 j++;
				}
			}
		Sleep (500);			// pause to answer
		CreateRegKeys (0);	// answer
		UL_INFO((LOGID, "Server running"));
	  }
	}
}
//-----------------------------------------------------------------------------------
DOUBLE cIEEE754toFloat (UCHAR *Data)
{
BOOL sign;CHAR exp;
DOUBLE res=0,zn=0.5, tmp;
UCHAR mask;
if (*(Data+1)&0x80) sign=TRUE; else sign=FALSE;
exp = ((*(Data+0)&0xff))-127;
for (INT j=1;j<=3;j++)		
	{	
	 mask = 0x80;
	 for (INT i=0;i<=7;i++)		
		{ 	 
		 if (j==1&&i==0) {res = res+1; mask = mask/2;}
		 else {
		 res = (*(Data+j)&mask)*zn/mask + res;
		 mask = mask/2; zn=zn/2; }
		}
	}
res = res * pow (2,exp);
tmp = 1*pow (10,-15);
if (res<tmp) res=0;
if (sign) res = -res;
return res;
}
//-----------------------------------------------------------------------------------