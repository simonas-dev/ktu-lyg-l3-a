package xyz.simonas;

import org.jcsp.lang.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;


public class Main {

    private static final String DATA_FULL = "SankauskasS_L3_1.txt";
    private static final String DATA_NO = "SankauskasS_L3_2.txt";
    private static final String DATA_SEMI = "SankauskasS_L3_3.txt";

    private static final String RESULT= "SankauskasS_L3a_rez.txt";

    private final Vector<SortedItem> B = new Vector<>();

    public static void main(String[] args) throws IOException {
        new Main().start(DATA_FULL);
//        new Main().start(DATA_NO);
//        new Main().start(DATA_SEMI);
    }

    public void start(String filePath) throws IOException {
        Vector<ItemCategory> readList = new Vector<>();
        Vector<ItemCategory> removeList = new Vector<>();
        readFile(filePath, readList, removeList);

        new ProcessHelper(readList, removeList).start();

        printRemainingData();

        System.out.println("Items remaining:");
        if (B.size() > 0) {
            for (SortedItem item : B) {
                System.out.println(item.name + ": " + item.count);
            }
        } else {
            System.out.println("Empty...");
        }
    }

    private void printRemainingData() throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("Remaining data:");
        for (SortedItem item : B) {
            lines.add(item.name + " | " + item.count + " | " + item.price);
        }
        Path path = new File(RESULT).toPath();
        Files.write(path, lines, StandardOpenOption.CREATE);
    }

    public void readFile(
            String path,
            Vector<ItemCategory> itemGroupList,
            Vector<ItemCategory> removeGroupList) throws IOException {
        List<String> lineStringList = Files.readAllLines(new File(path).toPath());
        Vector<String> data = new Vector<>(lineStringList);
        Scanner sc = new Scanner(data.get(0));

        int itemGroupCount = sc.nextInt();
        int deleteGroupCount = sc.nextInt();

        for (int i = 1; i < itemGroupCount+1; i++){
            Vector<Item> itemList = new Vector<>();
            sc = new Scanner(data.elementAt(i));
            String name = sc.next();
            int groupItemCount = sc.nextInt();
            for (int j = 1; j < groupItemCount+1; j++){
                sc = new Scanner(data.elementAt(i+j));
                String itemName = sc.next();
                int itemCount = sc.nextInt();
                double itemPrice = sc.nextDouble();
                itemList.add(new Item(itemName, itemCount, itemPrice));
            }
            itemGroupList.add(new ItemCategory(name, itemList));
            itemGroupCount += groupItemCount;
            i += groupItemCount;
        }

        for (int i = 0; i < deleteGroupCount; i++){
            Vector<Item> deleteItemList = new Vector<>();
            sc = new Scanner(data.elementAt(i+itemGroupCount+1));
            String name = sc.next();
            int groupItemCount = sc.nextInt();
            for(int g = 1; g < groupItemCount+1; g++) {
                sc = new Scanner(data.elementAt(i+itemGroupCount+1+g));
                String itemName = sc.next();
                int itemCout = sc.nextInt();
                double itemPrice = sc.nextDouble();
                deleteItemList.add(new Item(itemName, itemCout, itemPrice));
            }
            removeGroupList.add(new ItemCategory(name, deleteItemList));
            deleteGroupCount += groupItemCount;
            i += groupItemCount;
        }
    }

    class Item {
        public String name;
        public int count;
        public double price;

        public Item(String name, int count, double price) {
            this.name = name;
            this.count = count;
            this.price = price;
        }
    }

    static class SortedItem {
        public double price;
        public boolean isDelete = false;
        public State state = State.WRITING;
        public int count;
        public String name;

        enum State {
            WRITING,
            REMOVING,
            REMOVING_DONE,
            WRITING_DONE
        }

        public SortedItem(double price, int count, String name, boolean isDelete) {
            this.price = price;
            this.count += count;
            this.isDelete = isDelete;
            this.name = name;
        }

        public SortedItem(double price, int count, String name) {
            this.price = price;
            this.count += count;
            this.name = name;
        }

        public SortedItem(State state) {
            this.state = state;
        }
    }

    class ItemCategory {
        public String name;
        public Vector<Item> itemList;

        public ItemCategory(String name, Vector<Item> itemList) {
            this.name = name;
            this.itemList = itemList;
        }
    }

    class ReadProcess implements CSProcess {
        private ChannelOutput outputChannel;
        public ItemCategory itemCategory;

        public ReadProcess(ItemCategory itemCategory, ChannelOutput outputChannel) {
            this.itemCategory = itemCategory;
            this.outputChannel = outputChannel;
        }

        public void run() {
            for (int i = 0; i < itemCategory.itemList.size(); i++) {
                SortedItem item = new SortedItem(
                        itemCategory.itemList.elementAt(i).price,
                        itemCategory.itemList.elementAt(i).count,
                        itemCategory.itemList.elementAt(i).name
                );
                outputChannel.write(item);
            }

            outputChannel.write(new SortedItem(SortedItem.State.WRITING_DONE));
        }
    }

    class RemovalProcess implements CSProcess {
        private ChannelOutput outputChannel;
        private ChannelInput inputChannel;
        public ItemCategory itemCategory;

        public RemovalProcess(
                ItemCategory itemCategory,
                ChannelOutput outputChannel,
                ChannelInput inputChannel) {
            this.itemCategory = itemCategory;
            this.outputChannel = outputChannel;
            this.inputChannel = inputChannel;
        }

        public void run() {
            SortedItem[] sortedItemArr = new SortedItem[itemCategory.itemList.size()];

            for (int j = 0; j < itemCategory.itemList.size(); j++) {
                Item item = itemCategory.itemList.elementAt(j);
                sortedItemArr[j] = new SortedItem(item.price, item.count, item.name, true);
            }

            int retryCount = 0;
            boolean isWritingDone = true;

            while (retryCount < 10000 || isWritingDone) {
                if (B.size() == 0)
                    retryCount++;

                outputChannel.write(new SortedItem(SortedItem.State.REMOVING));

                int number = (int) inputChannel.read();

                if (number == 1) {
                    isWritingDone = false;
                }

                for (SortedItem sortedItem : sortedItemArr.clone()) {
                    for (SortedItem queryItem: new Vector<>(B)) {
                        if (sortedItem.name.equals(queryItem.name) && sortedItem.count <= queryItem.count) {
                            outputChannel.write(sortedItem);
                            retryCount = 0;
                        } else {
                            retryCount++;
                        }
                    }
                }
            }
            outputChannel.write(new SortedItem(SortedItem.State.REMOVING_DONE));
        }
    }

    class ControllerProcess implements CSProcess {
        private int writeCount;
        private int removeCount;
        private ChannelInput inputChannel;
        private ChannelOutput outputChannel;

        public ControllerProcess(
                ChannelInput inputChannel,
                int writeCount,
                int removeCount,
                ChannelOutput outputChannel) {
            this.writeCount = writeCount;
            this.removeCount = removeCount;
            this.inputChannel = inputChannel;
            this.outputChannel = outputChannel;
        }

        public void run() {
            int processDoneCount = 0;
            int writingDoneCount = 0;
            int totalCount = writeCount + removeCount;

            while(totalCount > processDoneCount) {
                SortedItem sortedItem = (SortedItem) inputChannel.read();
                if (sortedItem.state == SortedItem.State.REMOVING) {
                    int isWritingDone = 0;
                    if (writeCount == writingDoneCount) {
                        isWritingDone = 1;
                    }
                    outputChannel.write(isWritingDone);

                } else if (sortedItem.state == SortedItem.State.REMOVING_DONE) {
                    processDoneCount++;
                } else if (sortedItem.state == SortedItem.State.WRITING_DONE) {
                    processDoneCount++;
                    writingDoneCount++;
                } else if (!sortedItem.isDelete) {
                    SortedItem sortedItemCopy = new SortedItem(sortedItem.price, sortedItem.count, sortedItem.name);
                    if (B.size() ==  0) {
                        B.add(sortedItemCopy);
                    } else {
                        for (int i = 0; i < B.size(); i++) {
                            SortedItem loopItem = B.get(i);
                            if (loopItem.name.compareTo(sortedItemCopy.name) < 0) {
                                B.add(i, sortedItemCopy);
                                break;
                            }
                            if (loopItem.name.equals(sortedItemCopy.name)) {
                                B.get(i).count += sortedItemCopy.count;
                                break;
                            }
                            if (i == B.size() - 1) {
                                B.add(sortedItemCopy);
                            }
                        }
                    }
                } else if (sortedItem.isDelete) {
                    for (SortedItem delItem : new Vector<>(B)) {
                        if (delItem.name.equals(sortedItem.name) && delItem.count >= sortedItem.count) {
                            delItem.count -= sortedItem.count;
                            if (delItem.count <= 0) {
                                B.remove(delItem);
                            }
                        }
                    }
                }
            }
        }
    }

    class ProcessHelper {
        Any2OneChannel actionChannel = Channel.any2one();
        Any2OneChannel writeStatusChannel = Channel.any2one();
        ControllerProcess controllerProcess;
        ReadProcess[] readProcessArr;
        RemovalProcess[] removalProcessArr;
        
        int readItemCount = 0;
        int removeItemCount = 0;
        
        Parallel parallel = new Parallel();

        public ProcessHelper(List<ItemCategory> writeItemList, List<ItemCategory> removeItemList) {
            int readSize = writeItemList.size();
            int removeSize = removeItemList.size();

            readProcessArr = new ReadProcess[readSize];

            for (int i = 0; i < readSize; i++) {
                ReadProcess readProcess = new ReadProcess(
                        writeItemList.get(i),
                        actionChannel.out()
                );
                readProcessArr[i] = readProcess;
                readItemCount += writeItemList.get(i).itemList.size();
            }

            removalProcessArr = new RemovalProcess[removeSize];

            for (int i = 0; i < removeSize; i++) {
                RemovalProcess removalProcess = new RemovalProcess(
                        removeItemList.get(i),
                        actionChannel.out(),
                        writeStatusChannel.in()
                );
                removalProcessArr[i] = removalProcess;
                removeItemCount += writeItemList.get(i).itemList.size();
            }

            controllerProcess = new ControllerProcess(
                    actionChannel.in(),
                    readSize,
                    removeSize,
                    writeStatusChannel.out()
            );
        }

        public void start() {
            parallel.addProcess(controllerProcess);
            parallel.addProcess(readProcessArr);
            parallel.addProcess(removalProcessArr);
            parallel.run();
        }
    }
}
