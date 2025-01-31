def kiem_tra_tam_giac(a, b, c):

    if a <= 0 or b <= 0 or c <= 0:
        return "Không phải tam giác vì cạnh phải dương"

    if (a + b <= c) or (b + c <= a) or (a + c <= b):
        return "Không phải tam giác vì tổng hai cạnh phải lớn hơn cạnh còn lại"


    canh = sorted([a, b, c])
    if round(canh[0] ** 2 + canh[1] ** 2, 6) == round(canh[2] ** 2, 6):
        if a == b or b == c or a == c:
            return "Tam giác vuông cân"
        return "Tam giác vuông"


    if a == b == c:
        return "Tam giác đều"


    if a == b or b == c or a == c:
        return "Tam giác cân"


    return "Tam giác thường"


def main():
    while True:
        try:
            print("\nNhập 3 cạnh của tam giác:")
            a = float(input("Cạnh a = "))
            b = float(input("Cạnh b = "))
            c = float(input("Cạnh c = "))

            ket_qua = kiem_tra_tam_giac(a, b, c)
            print(f"\nKết quả: {ket_qua}")

            tiep_tuc = input("\nBạn có muốn tiếp tục không? (y/n): ")
            if tiep_tuc.lower() != 'y':
                break

        except ValueError:
            print("Lỗi: Vui lòng nhập số!")


if __name__ == "__main__":
    main()