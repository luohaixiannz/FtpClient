package uploader

import (
	"FtpClient/common"
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

// FilePart 文件片
type FilePart struct{
	Fid     string  // 操作文件ID，随机生成的UUID
	Index   int     // 文件切片序号
	Data    []byte  // 分片数据
}

// Uploader 上传器
type Uploader struct {
	common.FileMetadata    			// 文件元数据
	common.SliceSeq          		// 需要重传的序号
	waitGoroutine   sync.WaitGroup  // 同步goroutine
	NewLoader       bool            // 是否是新创建的上传器
	FilePath		string			// 上传文件路径
	SliceBytes		int				// 切片大小
	RetryChannel	chan *FilePart	// 重传channel通道
	MaxGtChannel	chan struct{}	// 限制上传的goroutine的数量通道
	StartTime		int64			// 上传开始时间
}

// UploadFile 单个文件的上传
func UploadFile(filePath string) error {
	targetUrl := common.BaseUrl + "upload"

	if !common.IsFile(filePath) {
		fmt.Printf("filePath:%s is not exist", filePath)
		return errors.New(filePath + "文件不存在")
	}

	filename := filepath.Base(filePath)
	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)
	fileWriter, err := bodyWriter.CreateFormFile("filename", filename)
	if err != nil {
		fmt.Println("error writing to buffer")
		return err
	}

	//打开文件句柄操作
	fh, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("error opening filePath: %s\n", filePath)
		return err
	}

	//iocopy
	_, err = io.Copy(fileWriter, fh)
	if err != nil {
		return err
	}
	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()
	resp, err := http.Post(targetUrl, contentType, bodyBuf)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("%s文件上传失败\n", filename)
		return errors.New("上传文件失败")
	}

	fmt.Printf("上传文件%s成功\n", filename)
	return nil
}

// NewUploader 新建一个上传器
func NewUploader(filePath string, sliceBytes int) (*Uploader) {
	uuid, err := uuid.NewUUID()
	if err != nil {
		fmt.Println("生成UUID失败")
		return nil
	}

	fileStat, err := os.Stat(filePath)
	if err != nil {
		fmt.Printf("读取文件%s失败, err: %s\n", filePath, err)
		return nil
	}

	filesize := fileStat.Size()
	if filesize <= 0 {
		fmt.Printf("%s文件是空文件，不能上传\n", filePath)
		return nil
	}

	// 计算文件切片数量
	sliceNum := int(math.Ceil(float64(filesize) / float64(sliceBytes)))

	metadata := common.FileMetadata{
		Fid:        uuid.String(),
		Filesize:   filesize,
		Filename:   filepath.Base(filePath),
		SliceNum:   sliceNum,
		Md5sum:     "",
		ModifyTime: fileStat.ModTime(),
	}

	uloader := &Uploader{
		FileMetadata:   metadata,
		SliceSeq:   	common.SliceSeq{
			Slices: []int{-1},
		},
		NewLoader:  	true,
		FilePath: 		filePath,
		SliceBytes: 	sliceBytes,
		RetryChannel: 	make(chan *FilePart, common.UploadRetryChannelNum),
		MaxGtChannel: 	make(chan struct{}, common.UpGoroutineMaxNumPerFile),
		StartTime: 		time.Now().Unix(),
	}

	err = common.StoreMetadata(getUploadMetaFile(filePath), &metadata)
	if err != nil {
		fmt.Println("新建上传器失败")
		return nil
	}
	return uloader
}

// GetUploader 获取一个上传器，用以初始化之前未上传完的
func GetUploader(filePath string, sliceBytes int) (*Uploader) {
	metaPath := getUploadMetaFile(filePath)
	if common.IsFile(metaPath) {
		file, err := os.Open(metaPath)
		if err != nil {
			fmt.Println("获取元数据文件状态失败")
			os.Remove(metaPath)
			return nil
		}

		var metadata common.FileMetadata
		filedata := gob.NewDecoder(file)
		err = filedata.Decode(&metadata)
		if err != nil {
			fmt.Println("格式化文件数据失败")
			os.Remove(metaPath)
			return nil
		}

		curFileStat, err := os.Stat(filePath)
		if err != nil {
			fmt.Println("获取文件状态失败")
			return nil
		}

		// 比较文件数据
		if metadata.Filesize != curFileStat.Size() || metadata.ModifyTime != curFileStat.ModTime() {
			fmt.Println("该文件已被修改过，全量重新上传")
			os.Remove(metaPath)
			return nil
		}

		uloader := &Uploader{
			FileMetadata:	metadata,
			FilePath: 		filePath,
			SliceBytes: 	sliceBytes,
			NewLoader: 		false,
			RetryChannel: 	make(chan *FilePart, common.UploadRetryChannelNum),
			MaxGtChannel: 	make(chan struct{}, common.UpGoroutineMaxNumPerFile),
			StartTime: 		time.Now().Unix(),
		}

		// 获取服务端需要我们重传的分片
		sliceSeq, err := uloader.getRetrySlice(metadata.Fid, metadata.Filename)
		if err != nil {
			sliceSeq = &common.SliceSeq{
				Slices: []int{-1},
			}
		}
		uloader.SliceSeq = *sliceSeq
		return uloader
	}

	// 不是正在上传的文件
	return nil
}

// 获取上传元数据文件路径
func getUploadMetaFile(filePath string) string {
	paths, fileName := filepath.Split(filePath)
	// 组合成隐藏文件名
	return path.Join(paths, "."+fileName+".uploading")
}

// 获取需要重新上传的序号，类似于SACK思想
func (*Uploader) getRetrySlice(fid string, filename string) (*common.SliceSeq, error) {
	targetUrl := common.BaseUrl + "getUploadingStat?fid=" + fid + "&filename=" + filename

	req, _ := http.NewRequest("GET", targetUrl, nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer resp.Body.Close()

	var seq common.SliceSeq
	err = json.NewDecoder(resp.Body).Decode(&seq)
	if err != nil {
		fmt.Println("获取重传序号失败")
		return nil, err
	}

	fmt.Println("还需上传的文件片：")
	fmt.Println(seq.Slices)
	return &seq, nil
}

// 向服务端发起请求，只需判断返回值是否成功即可
// 1.发起上传分片文件请求
// 2.发起合并分片文件请求
func (u *Uploader) sendCmdReq (targetUrl string) error {
	reqBody := new(bytes.Buffer)
	json.NewEncoder(reqBody).Encode(u.FileMetadata)
	req, err := http.NewRequest("POST", targetUrl, reqBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Printf("send data error")
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errMsg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(errMsg))
	}

	return nil
}

// 重传失败的分片
func (u *Uploader) retryUploadSlice() {
	for part := range u.RetryChannel {
		// 检查上传是否超时了，如果超时了则开始快速退出
		if time.Now().Unix() - u.StartTime > common.UploadTimeout {
			fmt.Println("上传超时，请重试")
			u.waitGoroutine.Done()
			continue
		}

		fmt.Printf("重传文件分片，文件名:%s, 分片序号:%d\n", u.Filename, part.Index)
		go u.uploadSlice(part)
	}
}

// 上传文件片
func (u *Uploader) uploadSlice(part *FilePart) error{
	// 控制上传文件片goroutine数量
	u.MaxGtChannel <- struct{}{}

	defer func (){
		<-u.MaxGtChannel
	}()

	targetUrl := common.BaseUrl + "uploadBySlice"
	//fmt.Printf("fid: %s, index: %d\n", part.Fid, part.Index)

	reqBody := new(bytes.Buffer)
	json.NewEncoder(reqBody).Encode(part)

	req, err := http.NewRequest("POST", targetUrl, reqBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Printf("上传文件分片失败，文件ID: %s, 序号：%d, err: %s\n", part.Fid, part.Index, err.Error())
		// 进行切片重传
		u.RetryChannel <- part
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errMsg, err := ioutil.ReadAll(resp.Body)
		msg := string(errMsg)
		if err != nil {
			msg = err.Error()
		}
		fmt.Printf("上传文件分片失败，文件ID: %s, 序号：%d, err: %s\n", part.Fid, part.Index, msg)
		u.RetryChannel <- part
		return errors.New(msg)
	}

	u.waitGoroutine.Done()
	return nil
}

// UploadFileBySlice 对文件切片并上传文件
func (u *Uploader) UploadFileBySlice() error {
	if u.NewLoader {
		// 新上传的文件才需要进行初始化
		err := u.sendCmdReq(common.BaseUrl + "startUploadSlice")
		if err != nil {
			fmt.Println(err.Error())
			os.Remove(getUploadMetaFile(u.FilePath))
			return err
		}
	}

	// 用来计算文件md5值
	md5sum := u.Md5sum

	if len(u.Slices) == 0 && md5sum != "" {
		// 分片都已保存在服务端了，提出合并请求即可
		err := u.sendCmdReq(common.BaseUrl + "mergeSlice")
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		os.Remove(getUploadMetaFile(u.FilePath))
		fmt.Printf("%s文件上传成功\n", u.Filename)
		return nil
	}

	//打开文件句柄操作
	fh, err := os.Open(u.FilePath)
	if err != nil {
		fmt.Printf("error opening filePath: %s\n", u.FilePath)
		return err
	}

	defer fh.Close()

	// 启动重传goroutine
	go u.retryUploadSlice()

	hash := md5.New()

	startIndex := 0
	if len(u.Slices) > 0  && u.Slices[0] >= 0 && md5sum != "" {
		startIndex = u.Slices[0]
	}
	// 跳过无须再读取的部分
	fh.Seek(int64(startIndex)*int64(u.SliceBytes), 0)

	for i := startIndex; i < u.SliceNum; i++ {
		tmpData := make([]byte, u.SliceBytes)
		nr, err := fh.Read(tmpData[:])
		if err != nil {
			fmt.Printf("read file error\n")
			return err
		}
		if md5sum == "" {
			hash.Write(tmpData[:nr])
		}
		tmpData = tmpData[:nr]

		if len(u.Slices) <= 0 {
			if md5sum == "" {
				// 还需计算md5
				continue
			}
			// 没有需要重传的了，直接跳出
			break
		} else if u.Slices[0] != -1 && i != u.Slices[0] {
			// 不需要重传的直接跳过
			continue
		}

		if u.Slices[0] != -1 {
			// 去掉重传的片
			u.Slices = u.Slices[1:]
		}

		// 构造切片并上传
		part := &FilePart{
			Fid:    u.Fid,
			Index:  i,
			Data:   tmpData,
		}
		u.waitGoroutine.Add(1)
		go u.uploadSlice(part)
	}

	if md5sum == "" {
		// 计算文件md5
		md5sum = hex.EncodeToString(hash.Sum(nil))
		// 保存md5到元数据文件
		u.Md5sum = md5sum
		err := common.StoreMetadata(getUploadMetaFile(u.FilePath), &u.FileMetadata)
		if err != nil {
			return err
		}
	}

	fmt.Println("等待分片上传完成")
	u.waitGoroutine.Wait()
	if time.Now().Unix() - u.StartTime > common.UploadTimeout {
		fmt.Println("上传超时，请重试")
		return errors.New("上传超时，请重试")
	}

	fmt.Println("分片都已上传完成")
	// 删除元数据文件
	defer os.Remove(getUploadMetaFile(u.FilePath))

	// 发起合并请求
	err = u.sendCmdReq(common.BaseUrl + "mergeSlice")
	if err != nil {
		fmt.Println("合并文件失败，请重新上传, err:", err.Error())
		return err
	}

	fmt.Printf("%s文件上传成功\n", u.Filename)
	return nil
}