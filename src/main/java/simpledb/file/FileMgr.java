package simpledb.file;

import java.io.*;
import java.util.*;

public class FileMgr {
    private File dbDirectory;
    private int blocksize;
    private boolean isNew;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blocksize) {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;
        isNew = !dbDirectory.exists();

        // create the directory if the database is new
        if (isNew)
            dbDirectory.mkdirs();

        // remove any leftover temporary tables
        for (String filename : dbDirectory.list())
            if (filename.startsWith("temp"))
                new File(dbDirectory, filename).delete();
    }

    /**
     * 将指定的 blk 读取到 page 中
     *
     * @param blk
     * @param p
     */
    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.getChannel().read(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    /**
     * 将指定 Page 中的内容写到文件的指定位置
     *
     * @param blk
     * @param p
     */
    public synchronized void write(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.getChannel().write(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block" + blk);
        }
    }

    /**
     * 给指定文件新增一个 block（预先占位）
     *
     * @param filename
     * @return
     */
    public synchronized BlockId append(String filename) {
        int newblknum = length(filename);
        BlockId blk = new BlockId(filename, newblknum);
        byte[] b = new byte[blocksize];
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.write(b);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }

    /**
     * 计算文件当前的块长度
     *
     * @param filename
     * @return
     */
    public int length(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int) (f.length() / blocksize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blocksize;
    }

    /**
     * 在当前数据库下文件
     *
     * @param filename
     * @return
     * @throws IOException
     */
    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile f = openFiles.get(filename);
        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }
}
