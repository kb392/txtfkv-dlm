//#define RSL_EXTERN_IMPL
#include <io.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
//#include <locale>
//#include <string>
//#include <fstream>
//#include <iostream>
#include <Windows.h>
//#include "mssup.h"
#include "rsl/dlmintf.h"


//#define TXTFKV_DEBUG_PRINT true


char * rsGetStringParam(int iParam, char * defStr) {
    VALUE *vString;
    if (!GetParm (iParam,&vString) || vString->v_type != V_STRING) {
        if(defStr)
            return defStr;
        else
            RslError("Параметр №%i должен быть строкой",(iParam+1));
        }
    return vString->value.string;
    }

char * rsGetFilePathParam(int iParam) {
    VALUE *vFilePath;
    if (!GetParm (iParam,&vFilePath) || vFilePath->v_type != V_STRING)
        RslError("Параметр №%i должен быть строкой",(iParam+1));
    char * sPath=(char *)malloc(sizeof(char)*strlen(vFilePath->value.string)+sizeof(char));
    OemToCharBuff(vFilePath->value.string, sPath, strlen(vFilePath->value.string));
    sPath[strlen(vFilePath->value.string)]='\0';
    return sPath;
    }

// номер позиции указанного символа в буфере определённой длины, если символа нет, то длина буфера
int memchri(const char * buff, char c, int buff_len) {
    for (int i=0;i<buff_len;i++) {
        if (buff[i]==c)
            return i;
        }
    return buff_len;
    }
// количество вхождений символа в буфере
int memcnt(const char * buff, char c, int buff_len) {
    int cnt=0;
    for (int i=0;i<buff_len;i++) 
        if (buff[i]==c)
            cnt++;
    return cnt;
    }


// юникодные переводы строк не поддерживаются
int strcrlf(const char * buff, int buff_len, int * piStrSepLen) {
    *piStrSepLen=0;
    for (int i=0;i<buff_len;i++) {
        if (buff[i]=='\r' || buff[i]=='\n') {
            if (buff[i]=='\r' && i+1<buff_len && buff[i+1]=='\n') {
                * piStrSepLen=2;
                return i;
                }
            * piStrSepLen=1;
            return i;
            }
        }

    return -1;
    }

class  TTxtKV {
    
    int openFile(void) {
        if (fh>0)
            return 1;

        if (!*m_fileName.value.string)
            return 0;

        if (page_buffer) 
            iDoneMem(page_buffer);
        page_buffer=(char*)iNewMem(page_size+4);
        page_buffer[page_size]='\0';

        fh = _open(m_fileName.value.string, _O_RDONLY | _O_BINARY); // C4996
        if( fh == -1 )
            RslError("file %s error %i",m_fileName.value.string,errno);

        buffer_data_size=_read(fh,page_buffer,page_size);

        setFileParams();

        return 1;

        }


    void setFileParams(void) {

        if (fh<0) 
            return;

        //int key_size=-1;
        for(int i=0;i<page_size;i++) {
            if (page_buffer[i]==separator || page_buffer[i]=='\r' || page_buffer[i]=='\n') {
                first_key_length=i;
                break;
                }
            }

        if (first_key_length==-1)
            RslError("Не найден ключ при инициализации файла");

        firstKey=(char*)iNewMem(first_key_length+1);
        memcpy(firstKey,page_buffer,first_key_length);
        firstKey[first_key_length]='\0';

        file_size = _lseeki64(fh, 0, SEEK_END);

        if (file_size<page_size) {
            // full scan
            }
        else {
            __int64 pos = _lseeki64(fh, -(__int64)page_size, SEEK_END);
            tmp_real_read_size=_read(fh,page_buffer,page_size);
            if(tmp_real_read_size!=page_size)
                RslError("Ошибка при чтении файла %s. Прочитано %i. Ожидалось %i. Позиция %I64d из %I64d. Ошибка %i",m_fileName.value.string,tmp_real_read_size,page_size,pos,file_size,errno);
            // reverse search

            bool flag_real_char=false;
            int separator_position=-1;
            tail_shift=0;
            for(int i=page_size-1;i>=0;i--) {
                // print ("%4i %c\n",i,page_buffer[i]);
                if (page_buffer[i]=='\r' || page_buffer[i]=='\n') {
                    if (flag_real_char) {
                        // нашли последнюю строку
                        if (separator_position>-1) {
                            last_key_length=separator_position-i;
                            }
                        else {
                            last_key_length=page_size-i-tail_shift+1;
                            }
                        lastKey=(char*)iNewMem(last_key_length);
                        memcpy(lastKey,page_buffer+i+1,last_key_length);
                        lastKey[last_key_length-1]='\0';
                        last_key_pos=pos+i+1;
                        break;
                        }
                    else {
                        // хвост
                        tail_shift++;
                        continue;
                        }
                    }
                if (page_buffer[i]==separator) {
                    separator_position=i;
                    }
                flag_real_char=true;
                }
            }
        }
        

    bool GetKey(__int64 iLine) {
        if (fh<0) return FALSE;

        __int64 pos=(iLine-1)*(lenStr+lenStrSep);
        if (pos<0)
            return FALSE;

        if (_lseeki64(fh, pos, SEEK_SET)!=pos)
            RslError("Ошибка позиционирования");

        if(_read(fh,currentKey,key_length)<key_length)
            RslError("read key %i from file %s error %i", (int)iLine, m_fileName.value.string, errno);

        currentKey[key_length]='\0';

        return TRUE;
        }


    void setResult2 (VALUE *pv, __int64 pos, __int64 pos_found) {
        int i;
        if (pos_found>=0) {
            i=(int)(pos_found-pos);
            if(page_buffer[i]=='\t') {
                for(int start=++i;i<page_size && i<tmp_real_read_size;i++) {
                    // если попался конец строки, то ставим вместо него сишный конец строки
                    // и возвращаем
                    if (page_buffer[i]=='\r' || page_buffer[i]=='\n') {
                        page_buffer[i]='\0';
                        ValueSet (pv,V_STRING, page_buffer+start);
                        return;
                        }
                    }
#ifdef TXTFKV_DEBUG_PRINT
                print("end page w/o page break. reread from pos %I64d\n", pos_found+1);
#endif
                // а если мы оказались здесь, то значит дошли до конца блока а конец строки не попался
                // перечитываем блок с новой позиции
                _lseeki64(fh, pos_found+1, SEEK_SET);
                tmp_real_read_size=_read(fh,page_buffer,page_size);
                for(i=0;i<tmp_real_read_size;i++) {
                    if (page_buffer[i]=='\r' || page_buffer[i]=='\n') {
                        page_buffer[i]='\0';
                        ValueSet (pv,V_STRING, page_buffer);
                        return;
                        }
                    }
                RslError("error R3 %I64d %I64d %I64d", pos, pos_found, file_size);


                }
            else if(page_buffer[i]=='\r' || page_buffer[i]=='\n') {
                int bool_true=1;
                ValueSet (pv,V_BOOL, &bool_true);
                return;
                }
            else if (pos+i==file_size) {
                page_buffer[(int)(pos_found-pos)+i]='\0';
                ValueSet (pv,V_STRING, page_buffer+(pos_found-pos));
                }
            else {
                page_buffer[(int)(pos_found-pos)+i]='\0';
                RslError("error R2 %i [%c] %I64d %I64d %I64d [%s]", i, page_buffer[i], pos, pos_found, file_size, page_buffer+(int)(pos_found-pos));
                }


            }
        }

    int scanPage(const char * key, __int64 pos, __int64 * p_found, __int64 * p_page_real_pos, __int64 * p_page_next_pos) {
        // bool flag_start=(pos==0);
        bool flag_pass_crlf=(pos==0);
        int start_pos=((pos==0)?0:-1);
        int r;
        int page_key_idx=-1;
        char tmp[256];
        *p_page_next_pos=-1;

        _lseeki64(fh, pos, SEEK_SET);
        tmp_real_read_size=_read(fh,page_buffer,page_size);
        int i;
        for(i=0;i<tmp_real_read_size;i++) {
            //print("i=%i, flag_pass_crlf=%i, start_pos=%i\n",i,flag_pass_crlf,start_pos);

            if (page_buffer[i]=='\r' || page_buffer[i]=='\n') { // конец строки
                flag_pass_crlf=true;
                if (start_pos<0) 
                    continue;
                else { // конец строки, при этом начало ключа отмечено
                    page_key_idx++;
                    r=strncmp(key,page_buffer+start_pos,i-start_pos);

                    memcpy(tmp,page_buffer+start_pos,i-start_pos);
                    tmp[i-start_pos]='\0';

#ifdef TXTFKV_DEBUG_PRINT
                    print("%i=[%c] index %i (%i), NL, %s<>%s=%i\n",i,page_buffer[i],page_key_idx,start_pos,key,tmp,r);
#endif
                    if (r<=0) { // ответ выше или нашли
                        if(page_key_idx>0 && r<0)
                            *p_found=-1;
                        else
                            *p_found=pos+i;
                        return r;
                        }
                    start_pos=-1;
                    }
                }
            else if (page_buffer[i]==separator) { // разделитель
                if(flag_pass_crlf) { // пустой ключ, что с ним делать?
                    RslError("Empty key at %I64d",pos+i);
                    }
                if (start_pos>=0) {
                    page_key_idx++;
                    memcpy(tmp,page_buffer+start_pos,i-start_pos);
                    tmp[i-start_pos]='\0';
                    r=strncmp(key,page_buffer+start_pos,i-start_pos);

#ifdef TXTFKV_DEBUG_PRINT
                    print("%i=[%c] index %i (%i), separator, %s<>%s=%i\n",i,page_buffer[i],page_key_idx,start_pos,key,tmp,r);
#endif
                    if (r<=0) { 
                        if(page_key_idx>0 && r<0)
                            *p_found=-1;
                        else
                            *p_found=pos+i;
                        return r;
                        }
                    start_pos=-1;
                    }
                flag_pass_crlf=false;
                }
            else { // значащий символ 
                if (flag_pass_crlf) { // первый символ после конца строки
                    start_pos=i;
                    *p_page_next_pos=pos+i;
                    if(page_key_idx==-1)
                        *p_page_real_pos=pos+i;
                    }
                flag_pass_crlf=false;
                }
            } // for

#ifdef TXTFKV_DEBUG_PRINT
            print("end page\n");
#endif
            if (flag_pass_crlf || (pos+i>=file_size)) { //если это страница кончается ВКПС или это последняя страница
                if (start_pos>=0) {
                    page_key_idx++;
                    r=strncmp(key,page_buffer+start_pos,i-start_pos);
                    if (r==0) { // ответ выше или нашли
                        if(page_key_idx>0 && r<0)
                            *p_found=-1;
                        else
                            *p_found=pos+i;
                        return r;
                        }
                    }
                }
            return 1;

        }

    bool findKey(const char * key, VALUE * retVal) {
        int r;
        __int64 result_pos=-1;
        __int64 page_real_pos=-1;
        __int64 pos;
        __int64 page;
        __int64 page_top=0;
        __int64 page_bottom=file_size/page_size+(file_size%page_size?1:0);
        __int64 prev_page=1;
        int prev_direction=0;
        __int64 pos_next=-1;

        if (file_size<=page_size) {
            r=scanPage(key, 0, &result_pos, &page_real_pos, &pos_next); 
            if (r==0 && result_pos>=0) {
                //print("нашли в %I64d\n",result_pos);
                setResult2 (retVal, 0, result_pos);
                return true;
                }
            return false;
            }

        r=strncmp(key,firstKey,first_key_length);
        if (r==0) { // нашли
#ifdef TXTFKV_DEBUG_PRINT
                print("first key\n");
#endif
            _lseeki64(fh, 0, SEEK_SET);
            tmp_real_read_size=_read(fh,page_buffer,page_size);
            setResult2 (retVal, 0, first_key_length);
            return true;
            }
        if (r<0) { // не найдём
            return false;
            }

        r=strncmp(key,lastKey,last_key_length);
        if (r==0) { // нашли
#ifdef TXTFKV_DEBUG_PRINT
                print("last key\n",prev_direction);
#endif
            _lseeki64(fh, last_key_pos, SEEK_SET);
            tmp_real_read_size=_read(fh,page_buffer,page_size);
            page_buffer[tmp_real_read_size]='\0';
#ifdef TXTFKV_DEBUG_PRINT
                print("%s\n",page_buffer);
                print("%s\n",page_buffer+last_key_length);
#endif
            setResult2 (retVal, last_key_pos, last_key_pos+last_key_length-1);
            return true;
            }
        if (r>0) { // не найдём
            return false;
            }

        bool flag_loop=true;
        step=0;

        while(flag_loop){

            step++;
            page=(page_bottom-page_top)/2+page_top;
            pos=page*page_size;
            if (page==prev_page) {
#ifdef TXTFKV_DEBUG_PRINT
                print("border mode %i\n",prev_direction);
#endif
                flag_loop=false;
                if (prev_direction<0) {
                    pos=page_real_pos-page_size-1;
                        if (pos<0) 
                            pos=0;
                    }
                else if (prev_direction>0) {
                    pos=pos_next-1;
                    }
                else
                    return false;
                }
#ifdef TXTFKV_DEBUG_PRINT
            print("step %i pos %I64d page %I64d %s\n",step,pos,page,key);
#endif
            r=scanPage(key, pos, &result_pos, &page_real_pos, &pos_next);
#ifdef TXTFKV_DEBUG_PRINT
            print("result %i pos %I64d, result_pos %I64d, page_real_pos %I64d, pos_next %I64d\n", r, pos, result_pos, page_real_pos, pos_next);
#endif

            if (r==0) {
                if (result_pos>=0) {
                    //print("нашли в %I64d\n",result_pos);
                    setResult2 (retVal, pos, result_pos);
                    return true;
                    }
                else {
                    return false;
                    }
                }
            if (r<0) {
                page_bottom=page;
                prev_direction=-1;
                }
            else if (r>0) {
                page_top=page;
                prev_direction=1;
                }
            else RslError("error FK");
            prev_page=page;

#ifdef TXTFKV_DEBUG_PRINT
            print("page top %I64d, page %I64d, page bottom %I64d, direction %i\n",page_top, page, page_bottom, prev_direction);
#endif
            };

        return false;
        }


public:

    TTxtKV (TGenObject *pThis = NULL) {
        
      ValueMake (&m_fileName);
      ValueSet (&m_fileName,V_STRING,"");

      //ValueMake (&m_tailShift);
      //m_tailShift.v_type.

      m_maxKeyLength.v_type=V_INTEGER;
      m_maxKeyLength.value.intval=256;

      
      }

    ~TTxtKV () {
        if (fh>-1)
            _close(fh); // уходя закройте файл
        if (*m_fileName.value.string)
            free(m_fileName.value.string);
        ValueClear(&m_fileName);

        if (firstKey)    iDoneMem(firstKey);
        if (lastKey)     iDoneMem(lastKey);
        if (strBuff)     iDoneMem(strBuff);
        if (currentKey)  iDoneMem(currentKey);
        if (resultStr)   iDoneMem(resultStr);
        if (page_buffer) iDoneMem(page_buffer);
        
        }

    RSL_CLASS(TTxtKV)

    RSL_INIT_DECL() {         // void TTxtKV::Init (int *firstParmOffs)
        
        m_fileName.value.string=rsGetFilePathParam(*firstParmOffs);
        char * separator_param=rsGetStringParam(*firstParmOffs+1, "\t");
        separator=*separator_param;

        openFile();
        }

    RSL_METHOD_DECL(Open) {

        return 0;
        }

    RSL_GETPROP_DECL(CountFields) {
        ValueClear (retVal);
        ValueSet (retVal,V_INTEGER,&count_fields);
        return 0;
        }

    RSL_GETPROP_DECL(KeyLength) {
        ValueClear (retVal);
        ValueSet (retVal,V_INTEGER,&key_length);
        return 0;
        }

    RSL_GETPROP_DECL(StringLength) {
        ValueClear (retVal);
        ValueSet (retVal,V_INTEGER,&lenStr);
        return 0;
        }

    RSL_GETPROP_DECL(Separator) {
        char RsSeparator[2];
        RsSeparator[0]=separator;
        RsSeparator[1]='\0';

        ValueClear (retVal);
        ValueSet (retVal,V_STRING,RsSeparator);
        return 0;
        }

    RSL_GETPROP_DECL(CountLines) {

        //long double dbl=lines_count;
        ValueClear (retVal);
        //retVal->v_type=V_DOUBLEL;
        //retVal->value.doubvalL=(long double)lines_count;
        retVal->v_type=V_DOUBLE;
        retVal->value.doubval=(double)lines_count;
        //ValueSet (retVal,V_DOUBLEL,&dbl); //doubvalL
        return 0;
        }

    RSL_GETPROP_DECL(FileSize) {
        ValueClear (retVal);
        retVal->v_type=V_DOUBLE;
        retVal->value.doubval=(double)file_size;
        return 0;
        }

    RSL_GETPROP_DECL(LastKey) {
        ValueClear (retVal);
        if (lastKey)
            ValueSet (retVal,V_STRING, lastKey);
        else
            ValueSet (retVal,V_UNDEF,lastKey);
        return 0;
        }

    RSL_GETPROP_DECL(FirstKey) {
        ValueClear (retVal);
        if (firstKey)
            ValueSet (retVal,V_STRING, firstKey);
        else
            ValueSet (retVal,V_UNDEF,firstKey);
        return 0;
        }

    RSL_METHOD_DECL(Find) {
        bool ret_bool=FALSE;
        const char * key =rsGetStringParam(1, NULL);

        if (findKey(key,retVal)) {
                /*
            if (count_fields)
                //ValueSet (retVal,V_STRING,resultStr);
                //ValueSet (retVal,V_STRING,"TEST");
                ;
            else {
                ret_bool=TRUE;
                ValueSet (retVal,V_BOOL,&ret_bool);
                }
                */
            return 0;
            }
        else {
            ValueSet (retVal,V_BOOL,&ret_bool);
            }

        return 0;
        }
                  


private:
    VALUE m_fileName;
    VALUE m_maxKeyLength;
    //VALUE m_tailShift;
    int fh=-1; // file handle 
    __int64 file_size=0;
    size_t page_size=4096;
    char * page_buffer=NULL;
    int buffer_data_size=0;
    int maxStringLength=256;
    int lenStr=-1;
    int lenStrSep=-1;
    char separator='\t';
    int key_length;
    char * firstKey  =NULL;
    char * lastKey   =NULL;
    __int64 last_key_pos=-1;
    int count_fields  =-1;
    char * strBuff   =NULL;
    char * currentKey=NULL;
    char * resultStr =NULL;
    int tail_shift=0;
    int last_key_length=-1;
    int first_key_length=-1;
    int step=0;
    int tmp_real_read_size=-1;

    __int64 lines_count=0;
};

TRslParmsInfo prmOneStr[] = { {V_STRING,0} };

RSL_CLASS_BEGIN(TTxtKV)
    RSL_INIT
    RSL_PROP_EX    (fileName,m_fileName,-1,V_STRING, VAL_FLAG_RDONLY)
    RSL_METH_EX    (Find,-1,V_UNDEF,0,RSLNP(prmOneStr),prmOneStr)
    RSL_PROP_METH  (CountFields)
    RSL_PROP_METH  (Separator)
    RSL_PROP_METH  (KeyLength)
    RSL_PROP_METH  (CountLines)
    RSL_PROP_METH  (StringLength)
    RSL_PROP_METH  (FileSize)
    RSL_PROP_METH  (LastKey)
    RSL_PROP_METH  (FirstKey)
    
RSL_CLASS_END  


EXP32 void DLMAPI EXP AddModuleObjects (void) {
    //setlocale(LC_ALL,".866");
    RslAddUniClass (TTxtKV::TablePtr,true);
    }




