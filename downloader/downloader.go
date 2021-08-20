package downloader

import (
	"FtpClient/common"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Downloader 下载器
type Downloader struct {
	common.FileMetadata                 // 文件元数据
	common.SliceSeq                     // 需要重传的序号
	waitGoroutine   sync.WaitGroup  	// 同步goroutine
	DownloadDir     string          	// 下载文件保存目录
	RetryChannel	chan int			// 重传channel通道
	MaxGtChannel	chan struct{}		// 限制上传的goroutine的数量通道
	StartTime		int64				// 下载开始时间
}

// DownloadFile 单个文件的下载
func DownloadFile(filename string, downloadDir string) (error){
	if !common.IsDir(downloadDir) {
		fmt.Printf("指定下载路径：%s 不存在\n", downloadDir)
		return errors.New("指定下载路径不存在")
	}

	targetUrl := common.BaseUrl + "download?filename=" + filename
	req, _ := http.NewRequest("GET", targetUrl, nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()

	filePath := path.Join(downloadDir, filename)
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf(err.Error())
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return err
	}
	fmt.Printf("%s 文件下载成功，保存路径：%s\n", filename, filePath)
	return nil
}

// NewDownLoader 新建一个下载器
func NewDownLoader(filename string, downloadDir string) (*Downloader) {
	targetUrl := common.BaseUrl + "getFileMetainfo?filename=" + filename

	req, _ := http.NewRequest("GET", targetUrl, nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer resp.Body.Close()

	var metadata common.FileMetadata
	err = json.NewDecoder(resp.Body).Decode(&metadata)
	if err != nil {
		fmt.Println("获取文件元数据失败")
		return nil
	}

	// 创建下载分片保存路径文件夹
	dSliceDir := path.Join(downloadDir, metadata.Fid)
	err = os.Mkdir(dSliceDir, 0766)
	if err != nil {
		fmt.Println("创建下载分片目录失败", dSliceDir, err)
		return nil
	}

	matadataPath := getDownloadMetaFile(path.Join(downloadDir, filename))
	err = common.StoreMetadata(matadataPath, &metadata)
	if err != nil {
		fmt.Println("写元数据文件失败")
		return nil
	}

	return &Downloader{
		DownloadDir:    	downloadDir,
		FileMetadata:       metadata,
		SliceSeq:       	common.SliceSeq{
			Slices: []int{-1},
		},
		RetryChannel: 		make(chan int, common.DownloadRetryChannelNum),
		MaxGtChannel: 		make(chan struct{}, common.DpGoroutineMaxNumPerFile),
		StartTime: 			time.Now().Unix(),
	}
}

// 获取上传元数据文件路径
func getDownloadMetaFile(filePath string) string {
	paths, fileName := filepath.Split(filePath)
	return path.Join(paths, "."+fileName+".downloading")
}

func GetDownLoader(filename string, downloadDir string) (*Downloader) {
	downloadingFile := getDownloadMetaFile(path.Join(downloadDir, filename))
	fmt.Println(downloadingFile)
	if common.IsFile(downloadingFile) {
		fmt.Printf("%s是还没下载完的文件", filename)
		file, err := os.Open(downloadingFile)
		if err != nil {
			fmt.Println("获取文件状态失败")
			return nil
		}

		var metadata common.FileMetadata
		filedata := gob.NewDecoder(file)
		err = filedata.Decode(&metadata)
		if err != nil {
			fmt.Println("格式化文件数据失败")
		}

		dloader := &Downloader{
			DownloadDir:    downloadDir,
			FileMetadata:   metadata,
			RetryChannel: 		make(chan int, common.DownloadRetryChannelNum),
			MaxGtChannel: 	make(chan struct{}, common.DpGoroutineMaxNumPerFile),
			StartTime: 		time.Now().Unix(),
		}

		// 计算还需下载的分片
		sliceseq, err := dloader.calNeededSlice()
		if err != nil {
			os.Remove(downloadingFile)
			return nil
		}
		dloader.Slices = sliceseq.Slices

		return dloader
	}

	// 不是正在下载的文件
	return nil
}

// 计算还需下载的分片序号
func (d *Downloader) calNeededSlice() (*common.SliceSeq, error) {
	seq := common.SliceSeq{
		Slices: []int{},
	}
	// 检查服务器端是否还存在这个文件
	targetUrl := common.BaseUrl + "checkFileExist?fid=" + d.Fid + "&filename=" + d.Filename

	req, _ := http.NewRequest("GET", targetUrl, nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer resp.Body.Close()
	// 判断状态码来判断是否检测成功
	if resp.StatusCode != http.StatusOK {
		fmt.Println("该文件在服务器端已不存在")
		os.Remove(path.Join(d.DownloadDir, d.Filename+".downloding"))
		return nil, errors.New("invalid downloading file")
	}

	// 获取已保存的文件片序号
	storeSeq := make(map[string]bool)
	files, _ := ioutil.ReadDir(path.Join(d.DownloadDir, d.Fid))
	for _, file := range files {
		_, err := strconv.Atoi(file.Name())
		if err != nil {
			fmt.Println("文件片有错", err, file.Name())
			continue
		}
		storeSeq[file.Name()] = true
	}

	i := 0
	for ;i < d.SliceNum && len(storeSeq) > 0; i++ {
		indexStr := strconv.Itoa(i)
		if _, ok := storeSeq[indexStr]; ok {
			delete(storeSeq, indexStr)
		} else {
			seq.Slices = append(seq.Slices, i)
		}
	}

	// -1指代slices的最大数字序号到最后一片都没有收到
	if i < d.SliceNum {
		seq.Slices = append(seq.Slices, i)
		i += 1
		if i < d.SliceNum {
			seq.Slices = append(seq.Slices, -1)
		}
	}

	fmt.Printf("%s还需重新下载的片\n", d.Filename)
	fmt.Println(seq.Slices)
	return &seq, nil
}

// 重下载失败的分片
func (d *Downloader) retryDownloadSlice() {
	for sliceIndex := range d.RetryChannel {
		// 检查下载是否超时了
		if time.Now().Unix() - d.StartTime > common.DownloadTimeout {
			fmt.Println("下载超时，请重试")
			d.waitGoroutine.Done()
		}

		fmt.Printf("重下载文件分片，文件名:%s, 分片序号:%d\n", d.Filename, sliceIndex)
		go d.downloadSlice(sliceIndex)
	}
}

// 下载分片
func (d *Downloader) downloadSlice(sliceIndex int) (error) {
	d.MaxGtChannel <- struct{}{}
	defer func (){
		<-d.MaxGtChannel
	}()

	targetUrl := common.BaseUrl + "downloadBySlice?filename=" + d.Filename + "&sliceIndex=" + strconv.Itoa(sliceIndex)
	req, _ := http.NewRequest("GET", targetUrl, nil)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		fmt.Println(err)
		d.RetryChannel <- sliceIndex
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		d.RetryChannel <- sliceIndex
		errMsg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(errMsg))
	}

	filePath := path.Join(d.DownloadDir, d.Fid, strconv.Itoa(sliceIndex))
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		d.RetryChannel <- sliceIndex
		return err
	}

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		fmt.Printf("文件%s的%d分片拷贝失败，失败原因:%s\n", d.Filename, sliceIndex, err.Error())
		d.RetryChannel <- sliceIndex
		return err
	}
	f.Close()
	//fmt.Printf("文件%s的%d分片下载成功, 写入字节数:%d\n", d.Filename, sliceIndex, writeByes)
	d.waitGoroutine.Done()
	return nil
}

// DownloadFileBySlice 切片方式下载文件
func (d *Downloader)DownloadFileBySlice() error {
	// 启动重下载goroutine
	go d.retryDownloadSlice()

	metadata := &d.FileMetadata
	for i :=0; i < metadata.SliceNum && len(d.Slices) > 0; i++ {
		if d.Slices[0] == -1 || i == d.Slices[0] {
			if d.Slices[0] != -1 {
				d.Slices = d.Slices[1:]
			}
			d.waitGoroutine.Add(1)
			go d.downloadSlice(i)
		}
	}

	// 等待各个分片都下载完成了
	fmt.Printf("%s等待分片下载完成\n", d.Filename)
	d.waitGoroutine.Wait()
	fmt.Printf("%s分片都已下载完成\n", d.Filename)
	return nil
}

// MergeDownloadFiles 合并分片文件为一个文件
func (d *Downloader) MergeDownloadFiles() error {
	fmt.Println("开始合并文件", d.Filename)
	targetFile := path.Join(d.DownloadDir, d.Filename)
	f, err := os.OpenFile(targetFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return err
	}

	sliceDir := path.Join(d.DownloadDir, d.Fid)

	// 计算md5值，这里要注意，一定要按分片顺序计算，不要使用读目录文件的方式，返回的文件顺序是无保证的
	md5hash := md5.New()

	defer os.Remove(getDownloadMetaFile(targetFile))
	defer os.RemoveAll(sliceDir)

	for i := 0; i < d.SliceNum; i++ {
		sliceFilePath := path.Join(sliceDir, strconv.Itoa(i))
		sliceFile, err := os.Open(sliceFilePath)
		if err != nil {
			fmt.Printf("读取文件%s失败, err: %s\n", sliceFilePath, err)
			return err
		}
		io.Copy(md5hash, sliceFile)

		// 偏移量需要重新进行调整
		sliceFile.Seek(0, 0)
		io.Copy(f, sliceFile)

		sliceFile.Close()
	}

	// 校验md5值
	calMd5 := hex.EncodeToString(md5hash.Sum(nil))
	if calMd5 != d.Md5sum {
		fmt.Printf("%s文件校验失败，请重新下载, 原始md5: %s, 计算的md5: %s\n", d.Filename, d.Md5sum, calMd5)
		return errors.New("文件校验失败")
	}
	f.Close()
	fmt.Printf("%s文件下载成功，保存路径：%s\n", d.Filename, targetFile)

	return nil
}
