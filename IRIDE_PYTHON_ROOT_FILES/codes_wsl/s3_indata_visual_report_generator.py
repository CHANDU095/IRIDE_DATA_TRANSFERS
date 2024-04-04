import os
from PIL import Image
import matplotlib.pyplot as plt

# Function to get all jpg files in a directory
def get_jpg_files(directory):
    jpg_files = []
    for filename in os.listdir(directory):
        if filename.endswith(".jpg"):
            jpg_files.append(os.path.join(directory, filename))
    return jpg_files

# Function to create a PDF with images
def create_pdf(images, pdf_name):
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(20, 18))  # Increase figure size

    # Adjust horizontal and vertical spacing between subplots
    plt.subplots_adjust(hspace=0.0, wspace=0.0)  # Decrease spacing

    for i, ax in enumerate(axes.flat):
        if i < len(images):
            img_path = images[i]
            img = Image.open(img_path)
            ax.imshow(img)
            ax.axis('off')
            #ax.set_title(os.path.basename(img_path)[:-4])

    plt.savefig(pdf_name, bbox_inches='tight',pad_inches=0.0)
    #plt.show()
    plt.close()

# Main function
def main():
    folder_path = "IRIDE_SNOWCOVER_DATAFUSION_S3/Final_SEN3_Tiffs/"

    output_dir=os.path.join("IRIDE_SNOWCOVER_DATAFUSION_S3","in_data_visual_report")
    os.makedirs(output_dir,exist_ok=True)
    pdf_name = "snowcover_images.pdf"
    all_images = []

    for date_folder in os.listdir(folder_path):
        date_folder_path = os.path.join(folder_path, date_folder)
        if os.path.isdir(date_folder_path):
            jpg_files = get_jpg_files(date_folder_path)
            if len(jpg_files) >= 4:
                create_pdf(jpg_files[:4], os.path.join(output_dir, f"{date_folder}.pdf"))
            all_images.extend(jpg_files)

    create_pdf(all_images, pdf_name)

if __name__ == "__main__":
    main()
